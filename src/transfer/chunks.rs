use crate::core::manifest::ChunkRecord;

pub const DEFAULT_CHUNK_SIZE: u64 = 1024 * 1024 * 2;

pub fn plan_chunks(file_size: u64, chunk_size: u64) -> Vec<ChunkRecord> {
    if file_size == 0 || chunk_size == 0 {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut start = 0;
    let mut index = 0;

    while start < file_size {
        let end = (start + chunk_size - 1).min(file_size - 1);
        chunks.push(ChunkRecord {
            index,
            start,
            end,
            done: false,
        });
        start = end + 1;
        index += 1;
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plans_last_chunk_without_overflowing_file() {
        let chunks = plan_chunks(5, 2);
        let ranges: Vec<_> = chunks.iter().map(|c| (c.start, c.end, c.len())).collect();
        assert_eq!(ranges, vec![(0, 1, 2), (2, 3, 2), (4, 4, 1)]);
    }
}
