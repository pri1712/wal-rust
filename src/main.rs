use std::fs::File;
use std::io::Write;

fn main() {
    println!("WAL application for learning basic rust");
    let mut file = File::create("logs.txt").expect("failed to create log file");
    let log1 = Log{
        key : String::from("a"),
        value : String::from("1"),
        checksum : 1
    };
    let bytes_written  = log1.write_data(&mut file).expect("issue in writing data to disk");
    println!("bytes written: {}", bytes_written);
}

struct Log {
    key: String,
    value: String,
    checksum: u32
}

impl Log {
    fn write_data(&self,file: &mut File) -> Result<usize, std::io::Error> {
        let record = format!("{}\t{}\t{}\n",self.key, self.value, self.checksum);
        let bytes = record.as_bytes();
        file.write_all(bytes)?;
        Ok(bytes.len())
    }

}

