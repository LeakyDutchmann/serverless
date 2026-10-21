use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::collections::HashMap;
use std::hash::{Hasher, Hash, DefaultHasher};

pub struct ShardedMap<K, V> {
    pub shards: Vec<RwLock<HashMap<K, V>>>
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

trait Merge {
    fn merge(&mut self, other: Self);
}

pub trait Sharded<K, V>
where 
    K: Hash + Eq + Clone,
    V: Clone 
{
    fn get_shard_idx(&self, key: &K) -> usize;
    async fn get(&self, key: &K) -> Option<V>;
    async fn insert(&self, key: K, value: V);
    async fn remove(&self, key: &K) -> Option<V>;
    async fn snapshot(&self) -> Vec<(K, V)>;
    async fn clear(&self);
    async fn len(&self) -> usize;
    async fn update(&self, key: &K, new_value: V) -> Option<V>;
    fn shards(&self) -> usize;
}

impl<K, V> Sharded<K, V> for ShardedMap<K, V>
    where 
        K: Hash + Eq + Clone, 
        V: Clone + Merge,
{
    fn get_shard_idx(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len()

    }
    async fn update(&self, key: &K, new_value: V) -> Option<V> {
        let idx = self.get_shard_idx(key);
        
        let mut map = self.shards[idx].write().await;
        if let Some(val) = map.get_mut(key) {
            val.merge(new_value);
            return Some(val.clone())
        }
        None
        
    }
    async fn get(&self, key: &K) -> Option<V> {
        let idx = self.get_shard_idx(&key);
        
        let map = self.shards[idx].read().await;
        if let Some(val) = map.get(key) {
            return Some(val.clone());
        }
        None
    }
    async fn insert(&self, key: K, value: V) {
        let idx = self.get_shard_idx(&key);
        let mut map = self.shards[idx].write().await;
        map.insert(key, value);
    }
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
