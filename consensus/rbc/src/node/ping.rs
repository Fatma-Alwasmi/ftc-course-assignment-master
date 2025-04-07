use types::{Msg, ProtMsg};

use super::Context;

impl Context {
    // A function's input parameter needs to be borrowed as mutable only when
    // we intend to modify the variable in the function. Otherwise, it need not be borrowed as mutable.
    // In this example, the mut can (and must) be removed because we are not modifying the Context inside
    // the function. 
     pub async fn start_ping(self: &mut Context){
         // Draft a message
         let msg = Msg{
             content: "Hi".as_bytes().to_vec(),
             origin: self.myid
         };
         // Wrap the message in a type
         // Use different types of messages like INIT, ECHO, .... for the Bracha's RBC implementation
         let protocol_msg = ProtMsg::Ping(msg, self.myid);
         // Broadcast the message to everyone
         self.broadcast(protocol_msg).await;
     }
    
    
    pub async fn start_rbc(self: &mut Context){
        if self.myid == 0 { //check if process is the leader
            //if so, send init msg
            let msg = format!("SEND:{}", String::from_utf8(self.inp_message.clone()).unwrap()); 
            // convert from str to bytes                                                                                    
            let bytes = msg.into_bytes(); 
            // wrap the msg in a protocl enum so its recognized as an RBC msg
            let wrapped = ProtMsg::Rbc(bytes);
            // broadcast msg
            self.broadcast(wrapped).await;  
        }
    }

    
    pub async fn handle_rbc(self: &mut Context, msg:Msg){
        //get msg, convert from bytes to str
        let msg_str = String::from_utf8(msg.content.clone()).unwrap(); 
        //---------------------SEND--------------------------//                                                                   
        // is it the init msg sent by the leader?
        if msg_str.starts_with("SEND:"){
            // get content of msg 
            let value = msg_str.strip_prefix("SEND:").unwrap().to_string();
            log::info!("Node {} Received SEND: {}", self.myid, value);
            //echo msg to all processes
            let echo_msg = format!("ECHO:{}", value).into_bytes();
            // wrap as RBC protocol msg
            let wrapped = ProtMsg::Rbc(echo_msg);
            // broadcast msg 
            self.broadcast(wrapped).await;
        }
        //---------------------ECHO--------------------------// 
        else if msg_str.starts_with("ECHO:"){
            // get content of msg
            let value = msg_str.strip_prefix("ECHO:").unwrap().to_string();
            log::info!("Node {} Received ECHO: {}", self.myid, value);
            // count ECHOs
            let echo_count = self.echo_count.entry(value.clone()).or_insert(0);
            *echo_count += 1;

            let n = self.num_nodes;
            let f = (n-1) / 3;

            if *echo_count >= (n-f){
                // only send ready once per value
                if !self.already_sent_ready.contains_key(&value){
                    self.already_sent_ready.insert(value.clone(), true);

                    let ready_msg = format!("READY:{}", value).into_bytes();
                    let wrapped = ProtMsg::Rbc(ready_msg);
                    self.broadcast(wrapped).await;
                }
            }
        }
        //---------------------READY--------------------------//
        else if msg_str.starts_with("READY:"){
            let value = msg_str.strip_prefix("READY:").unwrap().to_string();
            log::info!("Node {} Received READY: {}", self.myid, value);
            // count READYs
            let ready = self.ready_count.entry(value.clone()).or_insert(0);
            *ready += 1;
            let ready_count = *ready;
            
            // count ECHOs
            let echo = self.echo_count.entry(value.clone()).or_insert(0);
            *echo += 1;
            let echo_count = *echo;

            let n = self.num_nodes;
            let f = (n-1) / 3;

            if (echo_count >= (n-f) || ready_count >= (f+1)) && !self.already_sent_ready.contains_key(&value){

                self.already_sent_ready.insert(value.clone(), true);
                
                let ready_msg = format!("READY:{}", value).into_bytes(); 
                let wrapped = ProtMsg::Rbc(ready_msg);
                log::info!("Node {} broadcasting second READY: {}", self.myid, value);
                self.broadcast(wrapped).await;
            }

            if ready_count >= (n-f) {
                log::info!("Node {} calling terminate with value: {}", self.myid, value);
                self.terminate(value).await;
            }
        }
    }


     pub async fn handle_ping(self: &mut Context, msg:Msg){
         log::info!("Received ping message {:?} from node {}",msg.content,msg.origin);
         // Invoke this function after terminating the protocol. 
         //self.terminate("1".to_string()).await;
     }
}


