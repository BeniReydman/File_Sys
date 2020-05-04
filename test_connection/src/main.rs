use std::process;

extern crate paho_mqtt as mqtt;

fn main() {
        // Create a client & define connect options
        let cli = mqtt::Client::new("tcp://142.112.23.87:1883").unwrap_or_else(|err| {
            println!("Error creating the client: {:?}", err);
            process::exit(1);
        });
    
        let conn_opts = mqtt::ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(true)
            .finalize();
    
        // Connect and wait for it to complete or fail
        if let Err(e) = cli.connect(conn_opts).wait() {
            println!("Unable to connect:\n\t{:?}", e);
            process::exit(1);
        }
    
        // Create a message and publish it
        let msg = mqtt::Message::new("test", "Hello world!");
        let tok = cli.publish(msg);
    
        if let Err(e) = tok.wait() {
            println!("Error sending message: {:?}", e);
        }
    
        // Disconnect from the broker
        let tok = cli.disconnect();
        tok.wait().unwrap();
}
