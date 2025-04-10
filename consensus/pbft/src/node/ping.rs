//use std::collections::VecDeque;
use types::{Msg, ProtMsg};

use super::Context;

impl Context {
    
    pub async fn start_ping(&mut self) {
        let msg = Msg {
            content: "Hi".as_bytes().to_vec(),
            origin: self.myid
        };
        let protocol_msg = ProtMsg::Ping(msg, self.myid);
        self.broadcast(protocol_msg).await;
    }

    //-------------PBFT------------------
    pub async fn start_pbft(&mut self){

        let value_str = String::from_utf8(self.inp_message.clone()).unwrap();
        log::info!("Node {} starting PBFT with value {}", self.myid, value_str);

        let pbft_msg = ProtMsg::Pbft(value_str.clone(), self.myid);
        if self.myid == 0{

            let msg = Msg{
                content: value_str.clone().into_bytes(),
                origin: self.myid,
            };
            self.handle_pbft(msg).await;
        }
        else{
            //let wrapped = ProtMsg::pbft(pbft_msg);
            log::info!("Node {} sending value to leader", self.myid);
            let wrapped = WrapperMsg::new(pbft_msg, self.myid, &self.sec_key_map[&0]);
            self.send(0, wrapped).await;
            
        }
        
    }

    pub async fn handle_pbft(&mut self, msg: Msg){

        let content_str = String::from_utf8(msg.content.clone()).unwrap();

        if content_str.starts_with("PBFT_VALUE:") && self.myid == 0{
            let value = content_str.strip_prefix("PBFT_VALUE:").unwrap().to_string();
            log::info!("Leader received PBFT_VALUE: {} from node {}", value, msg.origin);

            self.pbft_values.insert(msg.origin, value);

            let required_nodes = (self.num_nodes*2)/3;
            if self.pbft_values.len() >= required_nodes{
                log::info!("Leader has recieved enough values {}/{}, starting agreement phase", self.pbft_values.len(), self.num_nodes);
                
                let values_vec: Vec<String> = self.pbft_values.values().cloned().filter_map(|v| v.parse::<f64>().ok()).collect();
                let values_str = serde_json::to_string(&values_vec).unwrap();
                
                self.inp_message = values_str.into_bytes();
                log::info!("leader initiating Bracha");
                self.start_rbc().await;

            }


        }
        
    }



    //----------------RBC---------------
    // Initiates Bracha RBC from node 0
    pub async fn start_rbc(&mut self){
        //am i the leader?
        if self.myid == 0 {
            //if so send the INIT message
            let msg_str = format!("SEND:{}", String::from_utf8(self.inp_message.clone()).unwrap());
            let bytes = msg_str.into_bytes();

            // Broadcast the SEND
            let wrapped = ProtMsg::Rbc(bytes.clone());
            self.broadcast(wrapped).await;

            // schedule a local RBC message
            let local_msg = Msg {
                content: bytes,
                origin: self.myid,
            };

            //push it into local_rbc_msgs
            self.local_rbc_msgs.push_back(local_msg);
        }
    }

    // RBC handler
    // processes one RBC message, then flushes any local RBC messages.
    pub async fn handle_rbc(&mut self, msg: Msg) {
        // 1. Process the inbound RBC message
        self.process_rbc_message(msg).await;

        // 2. flush all follow-up RBC messages we queued
        while let Some(local_msg) = self.local_rbc_msgs.pop_front() {
            self.process_rbc_message(local_msg).await;
        }
    }

    // broadcasts or pushes new local messages onto self.local_rbc_msgs.
    pub async fn process_rbc_message(&mut self, msg: Msg) {
        let msg_str = String::from_utf8(msg.content.clone()).unwrap();

        // --------------------- SEND ---------------------
        if msg_str.starts_with("SEND:") {
            let value = msg_str.strip_prefix("SEND:").unwrap().to_string();
            log::info!("Node {} Received SEND: {}", self.myid, value);

            // Only ECHO once per value
            if !self.already_sent_echo.contains_key(&value) {
                self.already_sent_echo.insert(value.clone(), true);

                let echo_bytes = format!("ECHO:{}", value).into_bytes();
                let wrapped = ProtMsg::Rbc(echo_bytes.clone());
                self.broadcast(wrapped).await;

                // Schedule local RBC for this ECHO
                let local_echo_msg = Msg {
                    content: echo_bytes,
                    origin: self.myid,
                };
                self.local_rbc_msgs.push_back(local_echo_msg);
            }
        }

        // --------------------- ECHO ---------------------
        else if msg_str.starts_with("ECHO:") {
            let value = msg_str.strip_prefix("ECHO:").unwrap().to_string();
            log::info!("Node {} Received ECHO: {}", self.myid, value);

            let echo_count = self.echo_count.entry(value.clone()).or_insert(0);
            *echo_count += 1;

            let n = self.num_nodes;
            let f = self.num_faults;

            // If we have enough ECHOs, we do READY
            if *echo_count >= (n - f) {
                // Only send READY once per value
                if !self.already_sent_ready.contains_key(&value) {
                    self.already_sent_ready.insert(value.clone(), true);

                    let ready_bytes = format!("READY:{}", value).into_bytes();
                    let wrapped = ProtMsg::Rbc(ready_bytes.clone());
                    self.broadcast(wrapped).await;

                    // Schedule local RBC for this READY
                    let local_ready_msg = Msg {
                        content: ready_bytes,
                        origin: self.myid,
                    };
                    self.local_rbc_msgs.push_back(local_ready_msg);
                }
            }
        }

        // --------------------- READY ---------------------
        else if msg_str.starts_with("READY:") {
            let value = msg_str.strip_prefix("READY:").unwrap().to_string();
            log::info!("Node {} Received READY: {}", self.myid, value);

            let ready_count = self.ready_count.entry(value.clone()).or_insert(0);
            *ready_count += 1;

            // increment ECHO count
            let echo_count = self.echo_count.entry(value.clone()).or_insert(0);
            *echo_count += 1;

            let n = self.num_nodes;
            let f = self.num_faults;

            // broadcast second READY
            if ( *echo_count >= (n - f) || *ready_count >= (f + 1) )
                && !self.already_sent_ready.contains_key(&value)
            {
                self.already_sent_ready.insert(value.clone(), true);

                let ready_bytes = format!("READY:{}", value).into_bytes();
                log::info!("Node {} broadcasting second READY: {}", self.myid, value);
                let wrapped = ProtMsg::Rbc(ready_bytes.clone());
                self.broadcast(wrapped).await;

                // Schedule local RBC for the second READY
                let local_second_ready = Msg {
                    content: ready_bytes,
                    origin: self.myid,
                };
                self.local_rbc_msgs.push_back(local_second_ready);
            }

            // If we have enough READY to finalize, do so
            let ready_count = self.ready_count.get(&value).unwrap();
            if *ready_count >= (n - f) {
                log::info!("Node {} calling terminate with value: {}", self.myid, value);
                //-------------pbft reciever
                let values: Vec<f64> = serde_json::from_str(&value).unwrap_or_default();

                if !values.is_empty(){
                    let mut sorted_values = values.clone();
                    sorted_values.sort_by(|a, b| a.partial_cmp(b).unwrap());

                    let median = if sorted_values.len() % 2 == 0 {

                        (sorted_values[sorted_values.len()/2 - 1] + sorted_values[sorted_values.len()/2]) / 2.0

                    }
                    else {
                        sorted_values[sorted_values.len()/2]
                    };
                    log::info!("Node {} calculated median: {}", self.myid, median);
                    self.terminate(median.to_string()).await;
                }

                else{
                    self.terminate(value).await;

                }
            }
        } 
        
    }


    pub async fn handle_ping(&mut self, msg: Msg){
        log::info!("Received ping message {:?} from node {}", msg.content, msg.origin);
        // self.terminate("1".to_string()).await;
    }
}

