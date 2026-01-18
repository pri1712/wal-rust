use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::io::Error;
use std::io::ErrorKind;
use std::mem::forget;
use crc32fast::Hasher;

//a log is considered durable only after the fsync call. before that its just in the cache.(even
// after that it could be in the ssd cache)
fn main() {
    if std::env::var("CRASH_WRITER").is_ok() {
        crash_writer();
        return;
    }

    if std::env::var("CRASH_READER").is_ok() {
        crash_reader();
        return;
    }
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
    fn write_multi_data(logs: &[Log],file: &mut File) -> Result<usize, std::io::Error> {
        let mut bytes_written = 0;
        for log in logs {
            bytes_written= bytes_written+ log.write_data(file)?;
        }
        Ok(bytes_written)
    }

    fn read_data<R: BufRead>(reader: &mut R) -> Result<Log, Error> {
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

        let checksum_int: u32 = checksum_str
            .trim()
            .parse()
            .expect("Error parsing checksum");

        let data = format!("{}{}", key, value);
        let mut hasher = Hasher::new();
        hasher.update(data.as_bytes());
        let checksum = hasher.finalize();

        if checksum_int != checksum {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "checksum mismatch",
            ));
        }

        Ok(Log { key, value })
    }

    fn read_multi_data(file: &mut File) -> Result<Vec<Log>, Error> {
        let mut reader = BufReader::new(file);
        let mut logs = Vec::new();
        loop {
            match Log::read_data(&mut reader) {
                Ok(log) => logs.push(log),
                Err(ref e) if e.kind() == ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e)
            }
        }
        Ok(logs)
    }
}

fn crash_writer() {
    //crashes after writing to buffer.
    let path = "crash_log.txt";

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("open failed");

    let logs = vec![
        Log { key: "k1".into(), value: "v1".into() },
        Log { key: "k2".into(), value: "v2".into() },
        Log { key: "k3".into(), value: "v3".into() },
        Log { key: "k4".into(), value: "v4".into() },
        Log { key: "k5".into(), value: "v5".into() },
    ];

    for log in logs {
        log.write_data(&mut file).expect("write failed");
    }
    std::process::exit(1);
}

fn crash_reader() {
    let path = "crash_log.txt";

    let mut file = OpenOptions::new()
        .read(true)
        .open(path)
        .expect("open failed");

    let logs = Log::read_multi_data(&mut file)
        .expect("replay failed");

    println!("{:?}", logs);
    println!("Recovered {} logs", logs.len());
}

mod tests {
    use super::*;
    #[test]
    fn test_single_write() {
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
    fn test_single_read() {
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
        Log::read_multi_data(&mut file).expect("read failed");
        assert_eq!(log.key, "k1");
        assert_eq!(log.value, "v1");
    }

    #[test]
    fn test_multi_write() {
        let path = "logs_multi.txt";

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .expect("Couldn't open logs_multi.txt");

        let logs = vec![
            Log { key: "k1".to_string(), value: "v1".to_string() },
            Log { key: "k2".to_string(), value: "v2".to_string() },
            Log { key: "k3".to_string(), value: "v3".to_string() },
            Log { key: "k4".to_string(), value: "v4".to_string() },
            Log { key: "k5".to_string(), value: "v5".to_string() },
        ];

        let bytes_written = Log::write_multi_data(&logs, &mut file)
            .expect("multi-write failed");

        assert!(bytes_written > 0);
    }
    #[test]
    fn test_multi_read() {
        let path = "logs_multi.txt";

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .expect("Couldn't open logs_multi.txt");

        let logs = vec![
            Log { key: "k1".to_string(), value: "v1".to_string() },
            Log { key: "k2".to_string(), value: "v2".to_string() },
            Log { key: "k3".to_string(), value: "v3".to_string() },
            Log { key: "k4".to_string(), value: "v4".to_string() },
            Log { key: "k5".to_string(), value: "v5".to_string() },
        ];

        let bytes_written = Log::write_multi_data(&logs, &mut file)
            .expect("multi-write failed");

        file.seek(SeekFrom::Start(0)).expect("seek failed");
        Log::read_multi_data(&mut file).expect("read failed");
        assert_eq!(logs.len(), 5);
    }
}