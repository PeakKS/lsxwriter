use clap::Parser;
use core::ffi;
use kiss_xml::{dom::Node, parse_stream};
use mmap_io::{MemoryMappedFile, MmapMode, segment::Segment};
use std::{
    collections::HashMap,
    error::Error,
    ffi::{CStr, CString},
    fmt::Display,
    fs::{self, File},
    hash::Hash,
    io::{self, BufWriter, Write},
    os::linux::raw,
    path::{Path, PathBuf},
    sync::Arc,
};
use topologic::AcyclicDependencyGraph;

// The actual game is a "mod" with the prefix "Gustav" (Seems to use both GustavX and GustavDev)
const BASE_GAME_MOD_PREFIX: &str = "Gustav";

// Byte order mark (Sometimes included at the start of an XML file)
const BOM: &str = "\u{feff}";

#[derive(Parser)]
#[command(version, about, long_about=None)]
struct Args {
    #[arg(
        long = "write",
        help = "True to write output modsettings.lsx file, false to output to stdout"
    )]
    write: bool,
    #[arg(help = "Directory game data is in (.../AppData/Local/Larian Studios/Baldur's Gate 3")]
    game_data: String,
}

#[derive(Debug)]
enum PAKError {
    BadMagic(String),
    NoMetadata,
}

impl std::error::Error for PAKError {}

impl Display for PAKError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadMagic(magic) => {
                write!(f, "Bad header magic value: {magic} (should be \"LSPK\")")
            }
            Self::NoMetadata => {
                write!(f, "Could not find meta.lsx file in pak")
            }
        }
    }
}

#[derive(Debug)]
struct PAKFile<'a> {
    file: Arc<MemoryMappedFile>,
    header: &'a PAKHeader,
    _file_list_data: Vec<u8>,
    file_list: &'a [PAKFileEntry],
}

impl<'a> Display for PAKFile<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "PAKFile: {}", self.file.path().display())?;
        writeln!(f, "File Count: {}", self.file_list.len())
    }
}

impl<'a> PAKFile<'a> {
    fn open(path: PathBuf) -> Result<PAKFile<'a>, Box<dyn Error>> {
        let file = MemoryMappedFile::open_ro(path)?;
        let header_slice = file.as_slice(0, size_of::<PAKHeader>().try_into()?)?;
        let header: &PAKHeader = unsafe { std::mem::transmute(header_slice.as_ptr()) };

        if header.magic != *b"LSPK" {
            return Err(Box::new(PAKError::BadMagic(format!("{:?}", header.magic))));
        }

        // Literally why
        let file_list_info = file
            .as_slice(header.file_list_offset, size_of::<u32>() as u64 * 2)
            .unwrap();

        let file_count =
            u32::from_le_bytes(file_list_info.get(0..4).unwrap().try_into().unwrap()) as usize;

        let compressed_size =
            u32::from_le_bytes(file_list_info.get(4..8).unwrap().try_into().unwrap());

        let file_list_slice = file
            .as_slice(header.file_list_offset + 8, compressed_size as u64)
            .unwrap();

        let uncompressed_size = size_of::<PAKFileEntry>() * file_count;

        let _file_list_data = lz4_flex::decompress(file_list_slice, uncompressed_size).unwrap();

        assert_eq!(
            file_count,
            _file_list_data.len() / size_of::<PAKFileEntry>()
        );

        let file_list = unsafe {
            let file_list_ptr: *const PAKFileEntry = std::mem::transmute(_file_list_data.as_ptr());
            std::slice::from_raw_parts(file_list_ptr, file_count)
        };

        Ok(Self {
            file: Arc::new(file),
            header,
            _file_list_data,
            file_list,
        })
    }

    fn module(&self) -> Result<Module, Box<dyn Error>> {
        let Some(metadata) = self.file_list.iter().find(|entry| {
            let name = entry.name();
            name.starts_with("Mods/") && name.ends_with("/meta.lsx")
        }) else {
            return Err(Box::new(PAKError::NoMetadata));
        };

        let compressed_meta = self
            .file
            .as_slice(metadata.offset(), metadata.size_on_disk as u64)
            .unwrap();

        let raw_meta =
            lz4_flex::decompress(compressed_meta, metadata.uncompressed_size as usize).unwrap();

        let str_meta = str::from_utf8(&raw_meta).unwrap();

        // Strip byte order mark if it exists
        let clean_str_meta = if str_meta.starts_with(BOM) {
            str_meta.get(BOM.len()..).unwrap()
        } else {
            str_meta
        };

        let xml_meta = kiss_xml::parse_str(clean_str_meta).unwrap();

        let children = xml_meta
            .root_element()
            .first_element_by_name("region")?
            .first_element_by_name("node")?
            .first_element_by_name("children")?;

        let mod_info = children
            .child_elements()
            .find(|child| child.get_attr("id").unwrap() == "ModuleInfo")
            .unwrap();

        let description = ModuleDescription::parse(mod_info);

        let dependencies = if let Some(dependencies) = children
            .child_elements()
            .find(|child| child.get_attr("id").unwrap() == "Dependencies")
            .unwrap()
            .first_element_by_name("children")
            .ok()
        {
            dependencies
                .child_elements()
                .map(|desc| ModuleDescription::parse(desc))
                .filter(|dep| {
                    if dep.name.starts_with(BASE_GAME_MOD_PREFIX) {
                        println!(
                            "Skipping dependency of {} on mod {} (base game)",
                            description.name, dep.name
                        );
                        false
                    } else {
                        true
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        Ok(Module {
            description,
            dependencies,
        })
    }
}

#[repr(C, packed)]
#[derive(Debug)]
struct PAKHeader {
    magic: [ffi::c_uchar; 4],
    version: u32,
    file_list_offset: u64,
    file_list_size: u32,
    flags: u8,
    priority: u8,
    md5: [ffi::c_char; 16],
    num_parts: u16,
}

#[repr(C, packed)]
#[derive(Debug)]
struct PAKFileEntry {
    name: [ffi::c_uchar; 256],
    offset_in_file_1: u32,
    offset_in_file_2: u16,
    archive_part: u8,
    flags: u8,
    size_on_disk: u32,
    uncompressed_size: u32,
}

impl PAKFileEntry {
    fn name(&self) -> String {
        let size = if let Some(terminator) = self.name.iter().position(|c| *c == 0) {
            terminator
        } else {
            self.name.len() - 1
        };

        let name_slice = self.name.get(0..size).unwrap();
        let mut name_vec = Vec::with_capacity(size);
        name_vec.extend_from_slice(name_slice);

        String::from_utf8(name_vec).unwrap()
    }

    fn offset(&self) -> u64 {
        self.offset_in_file_1 as u64 | ((self.offset_in_file_2 as u64) << 32)
    }
}

impl Display for PAKFileEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[derive(Debug, Default)]
struct Module {
    description: ModuleDescription,
    dependencies: Vec<ModuleDescription>,
}

#[derive(Debug, Default, Eq, Clone)]
struct ModuleDescription {
    folder: String,
    md5: String,
    name: String,
    publish_handle: Option<String>,
    uuid: String,
    version64: String,
}

impl Hash for ModuleDescription {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.uuid.hash(state);
    }
}

impl PartialEq for ModuleDescription {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}

impl ModuleDescription {
    fn parse(mod_element: &kiss_xml::dom::Element) -> Self {
        let folder = Self::get_attr(mod_element, "Folder").unwrap();
        let md5 = Self::get_attr(mod_element, "MD5").unwrap();
        let name = Self::get_attr(mod_element, "Name").unwrap();
        let publish_handle = Self::get_attr(mod_element, "PublishHandle");
        let uuid = Self::get_attr(mod_element, "UUID").unwrap();
        let version64 = Self::get_attr(mod_element, "Version64").unwrap();

        Self {
            folder,
            md5,
            name,
            publish_handle,
            uuid,
            version64,
        }
    }

    fn get_attr(mod_element: &kiss_xml::dom::Element, name: &str) -> Option<String> {
        let Some(att) = mod_element
            .child_elements()
            .find(|att| match att.get_attr("id") {
                Some(id) => id == name,
                None => false,
            })
        else {
            return None;
        };

        let Some(val) = att.get_attr("value") else {
            return None;
        };

        Some(val.to_string())
    }

    fn as_xml(&self) -> kiss_xml::dom::Element {
        let mut elem = kiss_xml::dom::Element::new_from_name("node").unwrap();

        elem.set_attr("id", "ModuleShortDesc").unwrap();
        elem.append(
            kiss_xml::dom::Element::new(
                "attribute",
                None,
                Some(HashMap::from([
                    ("id", "Folder"),
                    ("type", "LSString"),
                    ("value", &self.folder),
                ])),
                None,
                None,
                None,
            )
            .unwrap(),
        );

        elem.append(
            kiss_xml::dom::Element::new(
                "attribute",
                None,
                Some(HashMap::from([
                    ("id", "MD5"),
                    ("type", "LSString"),
                    ("value", &self.md5),
                ])),
                None,
                None,
                None,
            )
            .unwrap(),
        );

        elem.append(
            kiss_xml::dom::Element::new(
                "attribute",
                None,
                Some(HashMap::from([
                    ("id", "Name"),
                    ("type", "LSString"),
                    ("value", &self.name),
                ])),
                None,
                None,
                None,
            )
            .unwrap(),
        );

        let publish_handle_str = if let Some(publish_handle) = &self.publish_handle {
            publish_handle.clone()
        } else {
            String::from("0")
        };

        elem.append(
            kiss_xml::dom::Element::new(
                "attribute",
                None,
                Some(HashMap::from([
                    ("id", "PublishHandle"),
                    ("type", "uint64"),
                    ("value", &publish_handle_str),
                ])),
                None,
                None,
                None,
            )
            .unwrap(),
        );

        elem.append(
            kiss_xml::dom::Element::new(
                "attribute",
                None,
                Some(HashMap::from([
                    ("id", "UUID"),
                    ("type", "guid"),
                    ("value", &self.uuid),
                ])),
                None,
                None,
                None,
            )
            .unwrap(),
        );

        elem.append(
            kiss_xml::dom::Element::new(
                "attribute",
                None,
                Some(HashMap::from([
                    ("id", "Version64"),
                    ("type", "int64"),
                    ("value", &self.version64),
                ])),
                None,
                None,
                None,
            )
            .unwrap(),
        );

        elem
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let game_data = PathBuf::from(args.game_data);
    let mods_dir = game_data.join("Mods");

    let mods: Vec<PAKFile> = fs::read_dir(mods_dir)?
        .filter_map(|path| {
            let path = match path {
                Ok(path) => path,
                Err(_) => return None,
            };

            if let Some(name) = path.file_name().to_str()
                && name.ends_with(".pak")
            {
                match PAKFile::open(path.path()) {
                    Ok(module) => return Some(module),
                    Err(e) => eprintln!("Failed to parse pak file: {e}"),
                };
            }

            None
        })
        .collect();

    let mods: Vec<Module> = mods.iter().map(|pak| pak.module().unwrap()).collect();

    let modsettings_path = game_data.join("PlayerProfiles/Public/modsettings.lsx");
    let mut modsettings = kiss_xml::parse_stream(File::open(&modsettings_path)?).unwrap();

    let modlist = modsettings
        .root_element_mut()
        .first_element_by_name_mut("region")?
        .first_element_by_name_mut("node")?
        .first_element_by_name_mut("children")?
        .child_elements_mut()
        .find(|child| child.get_attr("id").unwrap() == "Mods")
        .unwrap()
        .first_element_by_name_mut("children")
        .unwrap();

    let base_elem = modlist
        .child_elements()
        .find(|old_mod| {
            let name = ModuleDescription::get_attr(old_mod, "Name").unwrap();

            // The actual game is a mod that starts with gustav (GustavX or GustavDev. Difference is???)
            name.starts_with(BASE_GAME_MOD_PREFIX)
        })
        .unwrap();

    let base_mod = ModuleDescription::parse(base_elem);
    println!("Found base module: {}", base_mod.name);

    let mut new_modlist = modlist.clone();

    // Rest mod list
    new_modlist.remove_elements_by_name("node");

    let mut dep_graph = AcyclicDependencyGraph::new();
    for module in mods {
        dep_graph
            .depend_on(module.description.clone(), base_mod.clone())
            .unwrap();
        for dependency in module.dependencies {
            dep_graph
                .depend_on(module.description.clone(), dependency)
                .unwrap();
        }
    }

    for layer in dep_graph.get_forward_dependency_topological_layers() {
        for module in layer {
            new_modlist.append(module.as_xml());
        }
    }

    *modlist = new_modlist;

    let mut writer: BufWriter<Box<dyn Write>> = if args.write {
        BufWriter::new(Box::new(File::create(&modsettings_path)?))
    } else {
        BufWriter::new(Box::new(io::stdout().lock()))
    };

    write!(writer, "{modsettings}").unwrap();

    writer.flush().unwrap();

    println!("Success");

    Ok(())
}
