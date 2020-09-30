use crate::QueryId;
use fnv::{FnvHashMap, FnvHashSet};
use ipfs_embed_core::{BlockNotFound, Cid, PeerId};
use std::collections::VecDeque;

#[derive(Debug)]
enum CidQueryState {
    InitialProviderSet,
    ProviderQuery,
    DhtProviderSet,
    Complete,
}

#[derive(Debug)]
struct CidQuery {
    cid: Cid,
    state: CidQueryState,
    have_queries: FnvHashSet<PeerId>,
    have_block: Option<FnvHashSet<PeerId>>,
    want_query: Option<PeerId>,
    events: VecDeque<CidQueryEvent>,
}

#[derive(Debug)]
pub enum QueryEvent {
    GetProviders(Cid),
    Have(PeerId, Cid),
    Want(PeerId, Cid),
}

#[derive(Debug)]
enum CidQueryEvent {
    Query(QueryEvent),
    Complete(Result<FnvHashSet<PeerId>, BlockNotFound>),
}

impl CidQuery {
    pub fn new(cid: Cid, initial_set: FnvHashSet<PeerId>) -> Self {
        let mut me = Self {
            cid,
            state: CidQueryState::InitialProviderSet,
            have_queries: Default::default(),
            have_block: Some(Default::default()),
            want_query: None,
            events: VecDeque::with_capacity(initial_set.len()),
        };
        me.start_have_queries(initial_set);
        me
    }

    fn start_have_queries(&mut self, set: FnvHashSet<PeerId>) {
        for peer_id in &set {
            if self.want_query.is_none() {
                self.want_query = Some(peer_id.clone());
                let query = QueryEvent::Want(peer_id.clone(), self.cid);
                self.events.push_back(CidQueryEvent::Query(query));
            } else {
                let query = QueryEvent::Have(peer_id.clone(), self.cid);
                self.events.push_back(CidQueryEvent::Query(query));
            }
        }
        self.have_queries = set;
    }

    fn start_want_query(&mut self) {
        if let Some(peer_id) = self.have_block.as_ref().unwrap().iter().next().cloned() {
            self.want_query = Some(peer_id.clone());
            let query = QueryEvent::Want(peer_id, self.cid);
            self.events.push_back(CidQueryEvent::Query(query));
        }
    }

    pub fn complete_have_query(&mut self, peer_id: PeerId, have: bool) {
        self.have_queries.remove(&peer_id);
        if have {
            self.have_block.as_mut().unwrap().insert(peer_id.clone());
        } else {
            self.have_block.as_mut().unwrap().remove(&peer_id);
            if self.want_query == Some(peer_id) {
                self.want_query = None;
            }
        }
        if self.want_query.is_none() {
            self.start_want_query();
        }
    }

    pub fn complete_get_providers(&mut self, providers: FnvHashSet<PeerId>) {
        self.state = CidQueryState::DhtProviderSet;
        self.start_have_queries(providers);
    }

    pub fn complete_want_query(&mut self, peer_id: PeerId) {
        self.state = CidQueryState::Complete;
        self.have_block.as_mut().unwrap().insert(peer_id);
    }

    pub fn next(&mut self) -> Option<CidQueryEvent> {
        if let Some(event) = self.events.pop_front() {
            return Some(event);
        }
        match self.state {
            CidQueryState::InitialProviderSet if self.have_queries.is_empty() => {
                self.state = CidQueryState::ProviderQuery;
                Some(CidQueryEvent::Query(QueryEvent::GetProviders(self.cid)))
            }
            CidQueryState::ProviderQuery => None,
            CidQueryState::DhtProviderSet if self.have_queries.is_empty() => {
                Some(CidQueryEvent::Complete(Err(BlockNotFound(self.cid))))
            }
            CidQueryState::Complete if self.have_queries.is_empty() => {
                Some(CidQueryEvent::Complete(Ok(self
                    .have_block
                    .take()
                    .unwrap_or_default())))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Default)]
pub struct QueryManager {
    queries: FnvHashMap<QueryId, CidQuery>,
    progress: FnvHashSet<QueryId>,
    mdns_peers: FnvHashSet<PeerId>,
}

impl QueryManager {
    pub fn get(&mut self, cid: Cid, query_id: QueryId) {
        let query = CidQuery::new(cid, self.mdns_peers.clone());
        self.queries.insert(query_id, query);
        self.progress.insert(query_id);
    }

    pub fn discover_mdns(&mut self, peer_id: PeerId) {
        self.mdns_peers.insert(peer_id);
    }

    pub fn expired_mdns(&mut self, peer_id: &PeerId) {
        self.mdns_peers.remove(peer_id);
    }

    pub fn complete_get_providers(&mut self, cid: Cid, providers: FnvHashSet<PeerId>) {
        for (id, query) in &mut self.queries {
            if query.cid == cid {
                query.complete_get_providers(providers.clone());
                self.progress.insert(*id);
            }
        }
    }

    pub fn complete_have_query(&mut self, cid: Cid, peer_id: PeerId, have: bool) {
        for (id, query) in &mut self.queries {
            if query.cid == cid {
                query.complete_have_query(peer_id.clone(), have);
                self.progress.insert(*id);
            }
        }
    }

    pub fn complete_want_query(&mut self, cid: Cid, peer_id: PeerId) -> Vec<QueryId> {
        let mut ids = vec![];
        for (id, query) in &mut self.queries {
            if query.cid == cid {
                query.complete_want_query(peer_id.clone());
                self.progress.insert(*id);
                ids.push(*id);
            }
        }
        ids
    }

    pub fn next(&mut self) -> Option<QueryManagerEvent> {
        loop {
            if let Some(id) = self.progress.iter().next().cloned() {
                match self.queries.get_mut(&id).unwrap().next() {
                    Some(CidQueryEvent::Query(query)) => {
                        return Some(QueryManagerEvent::Query(query));
                    }
                    Some(CidQueryEvent::Complete(Ok(_have))) => {
                        self.queries.remove(&id);
                        self.progress.remove(&id);
                        return Some(QueryManagerEvent::Complete(id, Ok(())));
                    }
                    Some(CidQueryEvent::Complete(Err(err))) => {
                        self.queries.remove(&id);
                        self.progress.remove(&id);
                        return Some(QueryManagerEvent::Complete(id, Err(err)));
                    }
                    None => {
                        self.progress.remove(&id);
                    }
                }
            } else {
                return None;
            }
        }
    }
}

#[derive(Debug)]
pub enum QueryManagerEvent {
    Query(QueryEvent),
    Complete(QueryId, Result<(), BlockNotFound>),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cid_query_block_not_found() {
        let mut initial_set = FnvHashSet::default();
        for _ in 0..3 {
            initial_set.insert(PeerId::random());
        }
        let mut provider_set = FnvHashSet::default();
        for _ in 0..3 {
            provider_set.insert(PeerId::random());
        }
        let cid = Cid::default();
        let mut query = CidQuery::new(cid, initial_set.clone());
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Want(_, _)))
        ));
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Have(_, _)))
        ));
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Have(_, _)))
        ));
        for peer in initial_set {
            assert!(matches!(query.next(), None));
            query.complete_have_query(peer, false);
        }
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::GetProviders(_)))
        ));
        assert!(matches!(query.next(), None));
        query.complete_get_providers(provider_set.clone());
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Want(_, _)))
        ));
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Have(_, _)))
        ));
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Have(_, _)))
        ));
        assert!(matches!(query.next(), None));
        for peer in provider_set {
            assert!(matches!(query.next(), None));
            query.complete_have_query(peer, false);
        }
        assert!(
            matches!(query.next(), Some(CidQueryEvent::Complete(Err(BlockNotFound(block)))) if block == cid)
        );
    }

    #[test]
    fn test_cid_query_block_initial_set() {
        let mut initial_set = FnvHashSet::default();
        for _ in 0..3 {
            initial_set.insert(PeerId::random());
        }
        let cid = Cid::default();
        let mut query = CidQuery::new(cid, initial_set.clone());
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Want(_, _)))
        ));
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Have(_, _)))
        ));
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Have(_, _)))
        ));
        for peer in initial_set.clone() {
            assert!(matches!(query.next(), None));
            query.complete_have_query(peer, true);
        }
        query.complete_want_query(initial_set.iter().next().cloned().unwrap());
        assert!(
            matches!(query.next(), Some(CidQueryEvent::Complete(Ok(set))) if set == initial_set)
        );
    }

    #[test]
    fn test_cid_query_block_provider_set() {
        let mut provider_set = FnvHashSet::default();
        for _ in 0..3 {
            provider_set.insert(PeerId::random());
        }
        let cid = Cid::default();
        let mut query = CidQuery::new(cid, Default::default());
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::GetProviders(_)))
        ));
        assert!(matches!(query.next(), None));
        query.complete_get_providers(provider_set.clone());
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Want(_, _)))
        ));
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Have(_, _)))
        ));
        assert!(matches!(
            query.next(),
            Some(CidQueryEvent::Query(QueryEvent::Have(_, _)))
        ));
        assert!(matches!(query.next(), None));
        for peer in provider_set.clone() {
            assert!(matches!(query.next(), None));
            query.complete_have_query(peer, true);
        }
        query.complete_want_query(provider_set.iter().next().cloned().unwrap());
        assert!(
            matches!(query.next(), Some(CidQueryEvent::Complete(Ok(set))) if set == provider_set)
        );
    }
}
