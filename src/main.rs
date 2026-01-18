use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::io::Error;
use std::io::ErrorKind;
fn main() {
    println!("WAL application for learning basic rust");
    let mut file = OpenOptions::new().read(true).write(true).create(true).open("logs.txt").expect("Couldn't open logs.txt");
    let log1 = Log{
        key : String::from("a"),
        value : String::from("1"),
        checksum : 1
    };
    let bytes_written  = log1.write_data(&mut file).expect("issue in writing data to disk");
    println!("bytes written: {}", bytes_written);
    file.seek(SeekFrom::Start(0)).expect("unable to seek to start");
    let read_log = Log::read_data(&mut file).expect("issue in reading data from disk");
    println!("read data: {:?}", read_log);
}

#[derive(Debug)]
struct Log {
    key: String,
    value: String,
    checksum: u32
}

impl Log {
    fn write_data(&self,file: &mut File) -> Result<usize, std::io::Error> {
        let record = format!("{}\n{}\n{}\n",self.key, self.value, self.checksum);
        let bytes = record.as_bytes();
        file.write_all(bytes)?;
        Ok(bytes.len())
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
        let checksum = checksum_str.trim().parse().unwrap();
        Ok(Log{
            key,
            value,
            checksum
        })
    }
}

