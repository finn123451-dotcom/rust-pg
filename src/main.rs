use std::path::PathBuf;
use heap_engine::*;

fn main() {
    println!("Heap Engine Test");
    
    let test_dir = PathBuf::from("test_heap");
    
    let result = HeapEngine::create(test_dir.clone(), 4);
    match result {
        Ok((mut engine, rel_id)) => {
            println!("Created heap relation with rel_id: {}", rel_id);
            
            engine.begin();
            println!("Started transaction");
            
            let data1 = b"test_data_1";
            match engine.insert(data1) {
                Ok(ctid) => {
                    println!("Inserted data at {:?}", ctid);
                    
                    let scan_result = engine.scan();
                    println!("Scan results: {:?}", scan_result);
                    
                    let get_result = engine.get(ctid);
                    println!("Get result: {:?}", get_result);
                    
                    let data2 = b"updated_data";
                    match engine.update(ctid, data2) {
                        Ok(new_ctid) => {
                            println!("Updated to new ctid: {:?}", new_ctid);
                        }
                        Err(e) => println!("Update error: {:?}", e),
                    }
                    
                    match engine.delete(ctid) {
                        Ok(deleted) => {
                            println!("Delete result: {}", deleted);
                        }
                        Err(e) => println!("Delete error: {:?}", e),
                    }
                }
                Err(e) => println!("Insert error: {:?}", e),
            }
            
            engine.commit();
            println!("Committed transaction");
            
            engine.close().unwrap();
            println!("Closed heap");
        }
        Err(e) => {
            println!("Error creating heap: {:?}", e);
        }
    }
    
    let _ = std::fs::remove_dir_all("test_heap");
}
