use std::fs::File;

fn main() {
    println!("WAL application for learning basic rust")

}

struct Log {
    Key: String,
    Value: String,
    Checksum: u32
}

impl Log {
    fn write_data(&self,file: &mut File) {
        //write data to the end of the file and increment the file offset.
        //on successful write return the number of bytes written, else return -1.

    }

    fn read_data(&self, file: &File, offset: isize) -> Log {
        //read the data from file at the given offset. calculate checksum of
        // data, if it does not match the checksum stored, return -1
    }
}