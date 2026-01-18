use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::io::Error;
use std::io::ErrorKind;
use std::mem::forget;
use crc32fast::Hasher;

fn main() {
    println!("WAL application for learning basic rust");
}

#[derive(Debug)]
struct Log {
    key: String,
    value: String
}

impl Log {
    fn write_data(&self,file: &mut File) -> Result<usize, std::io::Error> {
        let data = format!("{}\n{}\n",self.key,self.value);
        let data_bytes = data.as_bytes();
        let mut hasher = Hasher::new();
        hasher.update(data_bytes);
        let checksum = hasher.finalize();
        file.write_all(data_bytes)?;
        file.write_all(format!("{}\n", checksum).as_bytes())?;
        Ok(data_bytes.len() + checksum.to_string().len() + 1)
    }

    fn read_data(file: &mut File) -> Result<Log, Error> {
        let mut reader = BufReader::new(file);
        let mut key = String::new();
        let mut value = String::new();
        let mut checksum_str = String::new();
        let bytes = reader.read_line(&mut key)?;
        if bytes == 0 {
            return Err(Error::new(
                ErrorKind::UnexpectedEof,
                "end of log",
            ));
        }
        reader.read_line(&mut value)?;
        reader.read_line(&mut checksum_str)?;
        let checksum_int :u32 = checksum_str.trim().parse().expect("Unable to parse checksum");
        let data = format!("{}{}", key, value);
        let data_bytes = data.as_bytes();
        let mut hasher = Hasher::new();
        hasher.update(data_bytes);
        let checksum = hasher.finalize();
        println!("Checksum is {}",checksum);
        println!("Stored Checksum is {}",checksum_int);
        if checksum_int != checksum {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "checksum mismatch",
            ))
        }
        Ok(Log{
            key,
            value
        })
    }
}

mod tests {
    use super::*;
    #[test]
    fn test_simple_write() {
        //tests a basic write op.
        let path = "logs.txt";
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .expect("Couldn't open logs.txt");
        let log = Log {
            key : String::from("k1"),
            value : String::from("v1"),
        };
        let bytes_written = log.write_data(&mut file).expect("write failed");
        assert!(bytes_written > 0);
    }

    #[test]
    fn test_simple_read() {
        //write something and then try to read it back.
        let path = "logs.txt";
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .expect("Couldn't open logs.txt");
        let log = Log {
            key : String::from("k1"),
            value : String::from("v1"),
        };
        log.write_data(&mut file).expect("write failed");
        file.seek(SeekFrom::Start(0)).expect("seek failed");
        Log::read_data(&mut file).expect("read failed");
        assert_eq!(log.key, "k1");
        assert_eq!(log.value, "v1");
    }


}