pub fn compare_keys(a: &[u8], b: &[u8], key_size: usize) -> bool {
    if a.len() < key_size {
        if b[a.len()] != 0 {
            return false;
        }
    }

    for i in 0..a.len() {
        if a[i] != b[i] {
            return false;
        }
    }
    true
}
