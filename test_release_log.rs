use log::info;

fn main() {
    env_logger::init();
    info!("This is a test log message from release build");
    println!("This is a println message from release build");
}
