extern crate rmp_serde as rmps;
extern crate rand;
extern crate chrono;
extern crate ctrlc;
pub mod database;

use std::fs::File;
use std::fs::create_dir_all;
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::path::Path;
use std::{thread, time};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use rand::Rng;
use serde::{Serialize, Deserialize};
use rmps::{Serializer};
use std::io::Error;
use chrono::prelude::*;
// use crc::{crc32, Hasher32}; /* To be used once actual struct data is set */

#[derive(Serialize, Deserialize, Debug, PartialEq)]
#[allow(non_snake_case)]
pub struct RawData { // change all names
    pub AQHI:		Option<i32>,
	pub AQI:		Option<i32>,
	pub CO:			Option<f32>,
	pub CO2:		Option<f32>,
	pub NO:			Option<f32>,
	pub NO2:		Option<f32>,
	pub O3:			Option<f32>,
	pub PM1:		Option<f32>,
	pub PM2_5:		Option<f32>,
	pub PM10:		Option<f32>,
	pub SO2:		Option<f32>,
	pub T:			Option<f32>,
	pub RH:			Option<f32>,
	pub NOISE:		Option<f32>, 
	pub TimeStamp:	Option<String> // change ~ ticks
}

// User will configure a top level directory.

fn main() -> std::io::Result<()> {
    // Set DB
    let database = database::Database::new("data", "%Y%m%d", "%M");

    // Sleep Variables
    let sleep_time = time::Duration::from_millis(15000);

    // Ensure files are created
    create_dir_all("/data/levels")?;
    create_dir_all("/data/raw")?;

    // Set handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();

    ctrlc::set_handler(move || {
        println!("Quitting! Please wait at least 15 seconds.\n");
        r.store(false, Ordering::SeqCst);
    })
    .expect("Error setting Ctrl-C handler");
    
    while running.load(Ordering::SeqCst) {
        // Add directory if missing
        let mut directory1 = get_directory_lvls();
        create_dir_all(&mut directory1)?;

        // Apend time
        append_minute(&mut directory1);

        // Serialize
        let buf: Vec<u8> = new_buf().unwrap();

        // Check if exists
        if !Path::new(&directory1).exists() {
            File::create(&mut directory1)?;
            println!("File created!\n")
        }

        // Write and Read
        // update(directory1, buf);
        database.insert(database::Entry{sub_source: "/levels/", data: buf}).unwrap();
        //database::DB::find_file(&database,"data/levels").unwrap();

        // Sleep
        thread::sleep(sleep_time);
    }

    return Ok(());
}

/***
* Function updates:
*
* Purpose:
* Reads and writes to directory
***/
// fn update(mut directory: String, buf: Vec<u8>) {
//     // Write
//     {
//         write(&buf, &mut directory).unwrap();
//     }

//     // Read
//     {
//         let mut buf1 = Vec::<u8>::new();
//         read(&mut buf1, &mut directory).unwrap();
//     }

// }

/***
* Function new_buf:
*
* Purpose:
* Serialize a randomly generated struct
***/
fn new_buf() -> Result<Vec<u8>, Error> {
    match serialize_struct(generate_raw_data()) {
        Ok(buf) => return Ok(buf),
        Err(_) => return Err(Error::last_os_error())
    };
}

/***
* Function append_minute:
*
* Purpose:
* Appends the current local times minute to a directory
***/
fn append_minute(directory: &mut String) {
    let local: DateTime<Local> = Local::now();
    let minute = local.format("%M").to_string();
    directory.push_str("/");
    directory.push_str(&minute);
}

/***
* Function get_directory_lvls:
*
* Purpose:
* formats levels with current timestamp
***/
fn get_directory_lvls() -> String {
    return format!("data/levels/{}", get_date()).to_string();
}

/***
* Function get_directory_raw:
*
* Purpose:
* formats raw with current timestamp
***/
fn get_directory_raw() -> String {
    return format!("data/raw/{}", get_date()).to_string();
}

/***
* Function get_date:
*
* Purpose:
* returns current date timestamp
***/
fn get_date() -> String {
    let local: DateTime<Local> = Local::now();
    return local.format("%Y%m%d").to_string();
}

/***
* Function write:
*
* Purpose:
* appends buf to file
***/
fn write(buf: & Vec<u8>, mut directory: &mut String) -> std::io::Result<()> {
    let mut file = OpenOptions::new().append(true).open(&mut directory)?;
    file.write_all(&buf)?;
    println!("Wrote: {:?}\n", buf);
    Ok(())
}

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

/***
* Function generate_raw_data:
*
* Purpose:
* generates random data for struct

***/
fn generate_raw_data() -> RawData {
    let raw_data = RawData {
        AQHI:		generate_i32(),
        AQI:		generate_i32(),
        CO:			generate_f32(),
        CO2:		generate_f32(),
        NO:			generate_f32(),
        NO2:		generate_f32(),
        O3:			generate_f32(),
        PM1:		generate_f32(),
        PM2_5:		generate_f32(),
        PM10:		generate_f32(),
        SO2:		generate_f32(),
        T:			generate_f32(),
        RH:			generate_f32(),
        NOISE:		generate_f32(),
        TimeStamp:	Some("".to_string())
    };

    return raw_data;
}

/***
* Function generate_i32:
*
* Purpose:
* Generates random i32 from 0-10, if it's greater than 8, return null
***/
fn generate_i32() -> Option<i32> {
    let mut rng = rand::thread_rng();
    let num: i32 = rng.gen_range(0,10);
    if num >= 8 {
        return None;
    }
    return Some(num);
}

/***
* Function generate_f32:
*
* Purpose:
* Generates random f32 from 0-10, if it's greater than 8, return null
***/
fn generate_f32() -> Option<f32> {
    let mut rng = rand::thread_rng();
    let num: f32 = rng.gen_range(0.0,10.0);
    if num >= 8.0 {
        return None;
    }
    return Some(num);
}