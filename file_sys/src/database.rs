extern crate chrono;

use std::io;
use std::io::prelude::*;
use std::fs::File;
use std::fs::create_dir_all;
use std::fs::OpenOptions;
use chrono::prelude::*;

pub struct Database {
    source:         &'static str,
    date_format:    &'static str,
    time_format:    &'static str,
}

pub struct Entry {
    pub sub_source: &'static str,
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
    pub fn new(source: &'static str, data: &'static str, file: &'static str) -> Database {
        Database {
            source: source,
            date_format: data,
            time_format: file
        }
    }

    // Set a new source for the database
    pub fn set_source(&self, source: &str) -> Result<(), io::Error> {
        Ok(())
    }
    
    // Lists all the databases within the current data source
    pub fn list_db(&self) {

    }

    // Insert into database
    pub fn insert(&self, entry: Entry) -> Result<(), io::Error> {
        // Set the directory
        let directory = format!("{}/{}/{}/{}", 
                    self.source,                    // Database Directory
                    entry.sub_source,               // Sub directory
                    get_datetime(self.date_format), // Current format of data Ex: &Y&m&d -> 19700101
                    get_datetime(self.time_format)  // Current format of time
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

    // Find a partical Entry
    pub fn find_data(&self, date: &str) {
        
    }
}

fn get_datetime(format: &str) -> String {
    let local: DateTime<Local> = Local::now();
    return local.format(format).to_string();
}
