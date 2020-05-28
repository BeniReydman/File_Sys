extern crate chrono;

use std::convert::TryFrom;
use std::io;
use std::io::prelude::*;
use std::fs;
use std::fs::File;
use std::fs::create_dir_all;
use std::fs::OpenOptions;
use std::fs::metadata;
use chrono::prelude::*;

static DATE_FORMAT: &str = "%Y%m%d";
static TIME_FORMAT: &str = "%H";

pub struct Database {
    source:         &'static str,
}

pub struct Entry {
    pub table: &'static str,
    pub data:       Vec<u8>,
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
    pub fn insert(&self, entry: Entry) -> Result<(), io::Error> {
        // Set the directory
        let directory = format!("{}/{}/{}/{}", 
                    self.source,                    // Database Directory
                    entry.table,               // Sub directory
                    get_local_datetime(DATE_FORMAT), // Current format of data Ex: &Y&m&d -> 19700101
                    get_local_datetime(TIME_FORMAT)  // Current format of time
                );
        println!("{:?}", directory);

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

fn get_local_datetime(format: &str) -> String {
    let local: DateTime<Local> = Local::now();
    return local.format(format).to_string();
}

fn get_datetime(timestamp: u32) -> DateTime<Local> {
    let naive_datetime = NaiveDateTime::from_timestamp(i64::from(timestamp), 0);  // the 0 represents nanoseconds for leap seconds
    let utc_datetime = DateTime::<Utc>::from_utc(naive_datetime, Utc);
    let local: DateTime<Local> = Local::now();
    let datetime = utc_datetime.with_timezone(&local.timezone());
    return datetime;
}

fn get_timestamp() -> Option<u32> {
    let local: DateTime<Local> = Local::now();
    return u32::try_from(local.timestamp()).ok();
}