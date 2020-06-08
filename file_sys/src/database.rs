extern crate chrono;

use std::convert::TryFrom;
use std::io;
use std::io::prelude::*;
use std::fs;
use std::fs::File;
use std::fs::create_dir_all;
use std::fs::OpenOptions;
use std::fs::metadata;
use std::path::Path;
use std::io::{Error, ErrorKind};
use chrono::prelude::*;
use chrono::Duration;
use serde::{Serialize, Deserialize};
use crc::{crc32, Hasher32};
use rmps::{Serializer, Deserializer};

static DATE_FORMAT: &str = "%Y%m%d";
static TIME_FORMAT: &str = "%H";

pub struct Database {
    source:         &'static str,
}

pub struct Entry {
    pub table: &'static str,
    pub data:       Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
pub struct MpdRecordType {
    id:         u32,        // Record identifier
    datalog:    Vec<u8>,    // Byte array of length 'size'
    checksum:   u32,        // CRC-32 checksum of 'datalog'
}

pub trait DB {
    // Set a new source for the database
    fn set_source(&self, source: &str) -> Result<(), io::Error>;
    
    // Lists all the databases within the current data source
    fn list_db(&self);

    // Insert into database
    fn insert(&self, entry: Entry) -> Result<(), io::Error>;

    // Find a particular file
    fn find_file(&self, source: &str) -> Result<Vec<u8>, io::Error>;

    // Find a partical Entry
    fn find_data(&self, date: &str);
}

impl Database {
    // Constructor
    pub fn new(source: &'static str) -> Database {
        Database {
            source: source
        }
    }

    // Set a new source for the database
    pub fn set_source(&self, source: &str) -> Result<(), io::Error> {
        Ok(())
    }
    
    // Lists all the databases within the current data source
    pub fn list_db(&self) {
        print_directories(self.source, 0);
    }

    // Insert into database
    pub fn insert_at(&self, path: &str, file: &str, entry: Entry) -> Result<(), io::Error> {
        // Set the directory
        let mut directory = format!("{}/{}/{}", 
                    self.source,                    // Database Directory
                    entry.table,               // Sub directory
                    path  // Current format of time
                );
        println!("Directory is: {:?}", directory);

        
        // Ensure directory/file exists
        create_dir_all(&directory).unwrap();
        directory.push_str(&format!("/{}", file));
        let path = Path::new(&mut directory);
        if !path.exists() {
            File::create(&directory)?;
            println!("File created!\n");
        }

        // Set up data
        let new_data = MpdRecordType{
            id:         1,
            datalog:    entry.data.clone(),
            checksum:   crc32::checksum_ieee(&entry.data)
        };
        let serialized_data = serialize_struct(new_data).unwrap();

        // Write to database
        let mut file = OpenOptions::new().append(true).open(&directory)?;   // Write at end of file
        file.write_all(&serialized_data)?;
        println!("Wrote: {:?}\n", serialized_data);
        Ok(())
    }

    // Insert into database
    pub fn insert(&self, entry: Entry) -> Result<(), io::Error> {
        // Set the directory
        let directory = format!("{}/{}/{}/{}", 
                    self.source,                    // Database Directory
                    entry.table,               // Sub directory
                    get_local_datetime(DATE_FORMAT), // Current format of data Ex: &Y&m&d -> 19700101
                    get_local_datetime(TIME_FORMAT)  // Current format of time
                );
        println!("{:?}", directory);
        // Check if exists
        if !Path::new(&directory).exists() {
            File::create(&directory)?;
            println!("File created!\n")
        }
        

        // Write to database
        let mut file = OpenOptions::new().append(true).open(&directory)?;   // Write at end of file
        file.write_all(&entry.data)?;
        println!("Wrote: {:?}\n", entry.data);
        Ok(())
    }

    // Find a particular file/folder
    pub fn find_file(&self, source: &str) -> Result<Vec<u8>, io::Error> {
        // Set the directory
        let mut directory = String::new();
        directory.push_str(self.source);    // Database Directory
        directory.push_str(source);         // Sub directory

        // Read from file
        let mut buf: Vec<u8> = Vec::new();
        let mut file = File::open(&mut directory)?;
        file.read_to_end(&mut buf)?;
        println!("Read: {:?}\n", buf);
        return Ok(buf);
    }

    // Find a particular Entry
    pub fn find_data(&self, date: &str) {
        
    }

    pub fn get_data(&self, source: &'static str, start_time: u32, end_time: u32) {
        // Variables
        let mut curr_timestamp = get_datetime(start_time);
        let mut end_date = get_datetime(end_time);
        let mut buf = Vec::new();
        let mut curr_directory = String::new();
        let mut curr_file = String::new();

        // Find starting point
        while curr_timestamp <= end_date {
            // Setup variables
            buf.clear();
            curr_directory = format!("{}/{}/{}", self.source, source, curr_timestamp.format(DATE_FORMAT));
            curr_file = format!("{}/{}", curr_directory, curr_timestamp.format(TIME_FORMAT));

            /*** Check if Directory doesn't exist ***/
            if !Path::new(&curr_directory).exists() {
                // Add a day of time and continue
                curr_timestamp = curr_timestamp + Duration::seconds(86400);  // += gives error
                continue;
            }

            /*** Check if File doesn't exist ***/
            if !Path::new(&curr_file).exists() {
                // Add an hour of time and continue
                curr_timestamp = curr_timestamp + Duration::seconds(3600);  // += gives error
                continue;
            }
            
            /*** Read File ***/
            let mut file = File::open(curr_file).unwrap();
            file.read_to_end(&mut buf).unwrap();

            /*** Deserialize and "publish" ***/
            let mut de = Deserializer::new(&buf[..]);
            loop {
                let entry: MpdRecordType = match Deserialize::deserialize(&mut de) {
                    Ok(entry) => entry,
                    Err(_) => {
                        // Add an hour of time and continue
                        println!("Error!");
                        curr_timestamp = curr_timestamp + Duration::seconds(3600);  // += gives error
                        break;
                    }
                };

                // Send data here
                println!("{:?}", entry);

                // Check if entry ID is smaller than start_timestamp
                if entry.id < start_time {
                    continue;
                }

                // Check if entry ID is biiger than end_timestamp
                if entry.id > end_time {
                    return;
                }

                println!("Data is good!");
            }
        }
    }
}

// fn get_starting_point(source: &'static str, buf: &mut Vec<u8>, curr_timestamp: &mut DateTime<Utc>, end_time: &mut DateTime<Utc>) -> Result<DateTime<Utc>, Error> {
//     // Initialize variables
//     let mut curr_directory = format!("{}{}", source, curr_timestamp.format(DATE_FORMAT));
//     let mut curr_file = format!("{}{}", curr_directory, curr_timestamp.format(TIME_FORMAT));

//     // Find starting point
//     while curr_timestamp <= end_time {

//         /*** Check if Directory doesn't exist ***/
//         if !Path::new(&curr_directory).exists() {
//             // Add a day of time and continue
//             curr_timestamp = &mut (*curr_timestamp + Duration::seconds(86400));  // += gives error
//             continue;
//         }

//         /*** Check if File doesn't exist ***/
//         if !Path::new(&curr_file).exists() {
//             // Add an hour of time and continue
//             curr_timestamp = &mut (*curr_timestamp + Duration::seconds(3600));  // += gives error
//             continue;
//         }

//         /*** Read until first value is found ***/
//         let mut file = File::open(source)?;
//         file.read_to_end(&mut buf)?;

//         return Ok(*curr_timestamp);
//     }

//     return Err(Error::new(ErrorKind::Other, format!("No Data exists in the time range {:?} to {:?}.", start_time, end_time)));
// }

/***
* Function read:
*
* Purpose:
* reads directory
***/
fn read(mut buf: &mut Vec<u8>, mut directory: &mut String) -> std::io::Result<()> {
    let mut file = File::open(&mut directory)?;
    file.read_to_end(&mut buf)?;
    println!("Read: {:?}\n", buf);
    Ok(())
}

fn print_directories(path: &str, count: usize) {
    let paths = fs::read_dir(path).unwrap();

    for entry in paths {
        if let Ok(entry) = entry {
            if entry.path().is_dir() {
                // Print Directory
                print!("{:-<1$}", "", count);
                println!("{}", entry.file_name().into_string().unwrap());
                print_directories(entry.path().to_str().unwrap(), count + 1);
            }
        }
    }
}

fn print_db(path: &str, count: usize) {
    let paths = fs::read_dir(path).unwrap();

    for entry in paths {
        if let Ok(entry) = entry {
            // Print Directory
            print!("{:-<1$}", "", count);
            println!("{}", entry.file_name().into_string().unwrap());
            if entry.path().is_dir() {
                print_directories(entry.path().to_str().unwrap(), count + 1);
            }
        }
    }
}

/***
* Function serialize_struct:
*
* Purpose:
* Serializes structs
***/
fn serialize_struct<T>(data: T) -> Result<Vec<u8>, ()> where T: Serialize, {
    let mut buf = Vec::new();
    let mut msg_pack = Serializer::new(&mut buf);
    match data.serialize(&mut msg_pack) {
        Ok(_) => return Ok(buf),
        Err(e) => {
            println!("Error serializing: {:?}", e);
            return Err(())
        }
    }
}

fn get_local_datetime(format: &str) -> String {
    let local: DateTime<Utc> = Utc::now();
    return local.format(format).to_string();
}

// Convert timestamp to datetime
fn get_datetime(timestamp: u32) -> DateTime<Utc> {
    let naive_datetime = NaiveDateTime::from_timestamp(i64::from(timestamp), 0);  // the 0 represents nanoseconds for leap seconds
    let utc_datetime = DateTime::<Utc>::from_utc(naive_datetime, Utc);
    return utc_datetime;
}

fn get_timestamp() -> Option<u32> {
    let local: DateTime<Local> = Local::now();
    return u32::try_from(local.timestamp()).ok();
}

fn print_error(err: Error) {
    if let Some(inner_err) = err.into_inner() {
        println!("Inner error: {}", inner_err);
    } else {
        println!("No inner error");
    }
}