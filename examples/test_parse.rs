use megalib::public::open_folder;

#[tokio::main]
async fn main() {
    // Troque pela URL da pasta que quer inspecionar antes de rodar este exemplo.
    let url = "https://mega.nz/folder/ABCDEFGH#chaveDaPasta";
    match open_folder(url).await {
        Ok(folder) => {
            let sub = folder
                .nodes()
                .iter()
                .find(|n| n.handle == "HANDLEDASUBPASTA")
                .unwrap();
            let sub_path = sub.path().unwrap();
            println!("Subfolder path: {}", sub_path);
            let mut count = 0;
            for node in folder.nodes().iter().filter(|n| n.is_file()) {
                if let Some(path) = node.path() {
                    if path.starts_with(sub_path) {
                        if count < 5 {
                            println!("File: {}", path);
                        }
                        count += 1;
                    }
                }
            }
            println!("Total files in subfolder: {}", count);
        }
        Err(e) => {
            println!("Error: {}", e);
        }
    }
}
