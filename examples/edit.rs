use edit_s3_file::Part;

fn main() {
    let part = Part::new(1024, vec![1; 1024 * 1024 * 10]);

    edit_s3_file::modify(
        "plot.bin",
        part
    ).unwrap();
}