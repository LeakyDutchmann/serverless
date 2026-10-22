use tokio::sync::RwLock;
use std::collections::HashMap;
use std::hash::{Hasher, Hash, DefaultHasher};

pub struct ShardedMap<K, V> {
    shards: Vec<RwLock<HashMap<K, V>>>
}

impl<K, V> ShardedMap<K, V> {
    pub fn new(shards: usize) -> Self {
        let mut shmap = ShardedMap {
            shards: Vec::with_capacity(shards)
        };
        for _ in 0..shards {
            let shard = RwLock::new(HashMap::new());
            shmap.shards.push(shard);
        }
        shmap
    }
}

pub trait Sharded<K, V>
where 
    K: Hash + Eq + Clone,
    V: Clone 
{
    fn get_shard_idx(&self, key: &K) -> usize;
    fn get(&self, key: &K) -> impl Future<Output = Option<V>>;
    fn insert(&self, key: K, value: V) -> impl Future<Output = ()>;
    fn remove(&self, key: &K) -> impl Future<Output = Option<V>>;
    fn snapshot(&self) -> impl Future<Output = Vec<(K, V)>>;
    fn clear(&self) -> impl Future<Output = ()>;
    fn len(&self) -> impl Future<Output = usize>;
    fn shards(&self) -> usize;
    async fn modify<F>(&self, key: &K, f: F) -> Option<V>
    where
        F: FnOnce(&mut V);
}

impl<K, V> Sharded<K, V> for ShardedMap<K, V>
    where 
        K: Hash + Eq + Clone, 
        V: Clone,
{
    fn get_shard_idx(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()

    }
    ///This method calls a updates stored value according to passed in closure and returns new value. If there's no value present in map, returns None
    async fn modify<F>(&self, key: &K, f: F) -> Option<V>
    where
        F: FnOnce(&mut V)
    {
        let idx = self.get_shard_idx(key);
        let mut map = self.shards[idx].write().await;
    
        if let Some(val) = map.get_mut(key) {
            f(val);
            return Some(val.clone());
        }
        None
    }
    ///Returns clone of a value stored in map from shard determined by hash of key
    async fn get(&self, key: &K) -> Option<V> {
        let idx = self.get_shard_idx(&key);
        
        let map = self.shards[idx].read().await;
        if let Some(val) = map.get(key) {
            return Some(val.clone());
        }
        None
    }
    ///Inserts key-value pair into map from shard that is determined by hash of key. If value is present, then it is replaced
    async fn insert(&self, key: K, value: V) {
        let idx = self.get_shard_idx(&key);
        let mut map = self.shards[idx].write().await;
        map.insert(key, value);
    }
    ///Removes and returns a value stored in map from shard determined by hash of key
    async fn remove(&self, key: &K) -> Option<V> {
        let idx = self.get_shard_idx(key);
        let mut map = self.shards[idx].write().await;
        if let Some(val) = map.remove(key) {
            return Some(val);
        }
        None
    }
    async fn len(&self) -> usize {
        let mut len = 0;
        for shard in &self.shards {
            let lock = shard.read().await;
            len += lock.len();
        }
        len
    }
    ///Collects clones of all items from maps into single vec
    async fn snapshot(&self) -> Vec<(K, V)> {
        let mut vec = Vec::new();
        for shard in &self.shards {
            let lock = shard.read().await;
            for (k, v) in lock.iter() {
                vec.push((k.clone(), v.clone()))
            }
        }
        vec
    }
    async fn clear(&self) {
        for shard in &self.shards {
            let mut lock = shard.write().await;
            lock.clear();
        }
    }
    fn shards(&self) -> usize {
        self.shards.len()
    }
}

#[cfg(test)]
mod sharded_tests {
    use super::*;
    
    #[test]
    fn test_idx() {
        let map: ShardedMap<String, usize> = ShardedMap::new(12);
        let strings: Vec<String> = vec!["logtx", "kick", "boom", "black", "krow", "geek"].iter().map(|v| v.to_string()).collect();
        let mut counts = vec![0usize; 12];
        for s in strings.iter() {
            let idx = map.get_shard_idx(s);
            counts[idx] += 1;
        }
        for c in counts {
            assert!(c <= 2);
        }

        let s = String::from("patH");
        let hash = map.get_shard_idx(&s);
        for _ in 0..5 {
            let idx = map.get_shard_idx(&s);
            assert_eq!(hash, idx);
        }
    }

    #[tokio::test]
    async fn test_get_insert_update() {
        let map: ShardedMap<String, usize> = ShardedMap::new(64);
        
        let strings: Vec<String> = vec!["logtx", "kick", "boom", "black", "krow", "geek"].iter().map(|v| v.to_string()).collect();

        for (i, s) in strings.iter().enumerate() {
            map.insert(s.to_string(), i).await;
            let v = map.get(s).await;
            assert_eq!(i, v.unwrap());
            map.modify(s, |v| *v += 1).await;
            let v = map.get(s).await;
            assert_eq!(i + 1, v.unwrap());
            map.remove(s).await;
            assert_eq!(None, map.get(s).await);
        }
    }

    #[tokio::test]
    async fn test_snapshot() {
        let map: ShardedMap<String, usize> = ShardedMap::new(12);

        let strings: Vec<String> = vec!["logtx", "kick", "boom", "black", "krow", "geek"].iter().map(|v| v.to_string()).collect();
        for (i, s) in strings.iter().enumerate() {
            map.insert(s.to_string(), i).await;
        }
        let snapshot = map.snapshot().await;
        for (s, usize) in snapshot {
            assert_eq!(strings[usize], s)
        }
    }
}