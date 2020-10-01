use fnv::FnvHashSet;
use ipfs_embed_core::{BlockNotFound, Cid, PeerId, Query, QueryResult};
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

impl std::borrow::Borrow<Cid> for CidQuery {
    fn borrow(&self) -> &Cid {
        &self.cid
    }
}

impl std::hash::Hash for CidQuery {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        std::hash::Hash::hash(&self.cid, h);
    }
}

impl PartialEq for CidQuery {
    fn eq(&self, other: &Self) -> bool {
        self.cid == other.cid
    }
}

impl Eq for CidQuery {}

#[derive(Debug)]
pub enum QueryEvent {
    GetProviders(Cid),
    Have(PeerId, Cid),
    Want(PeerId, Cid),
    Complete(Query, QueryResult),
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
        self.have_queries.remove(&peer_id);
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
    queries: FnvHashSet<CidQuery>,
    progress: FnvHashSet<Cid>,
    mdns_peers: FnvHashSet<PeerId>,
}

impl QueryManager {
    pub fn start(&mut self, query: Query) {
        if !self.queries.contains(query.as_ref()) {
            let get_query = CidQuery::new(*query.as_ref(), self.mdns_peers.clone());
            self.queries.insert(get_query);
            self.progress.insert(*query.as_ref());
        }
    }

    pub fn discover_mdns(&mut self, peer_id: PeerId) -> bool {
        if !self.mdns_peers.contains(&peer_id) {
            log::debug!("discovered {}", peer_id);
        }
        self.mdns_peers.insert(peer_id)
    }

    pub fn expired_mdns(&mut self, peer_id: &PeerId) {
        if self.mdns_peers.remove(peer_id) {
            log::debug!("expired {}", peer_id);
        }
    }

    pub fn complete_get_providers(&mut self, cid: Cid, providers: FnvHashSet<PeerId>) {
        if let Some(mut query) = self.queries.take(&cid) {
            query.complete_get_providers(providers);
            self.queries.insert(query);
            self.progress.insert(cid);
        }
    }

    pub fn complete_have_query(&mut self, cid: Cid, peer_id: PeerId, have: bool) {
        if let Some(mut query) = self.queries.take(&cid) {
            query.complete_have_query(peer_id.clone(), have);
            self.queries.insert(query);
            self.progress.insert(cid);
        }
    }

    pub fn complete_want_query(&mut self, cid: Cid, peer_id: PeerId) {
        if let Some(mut query) = self.queries.take(&cid) {
            query.complete_want_query(peer_id);
            self.queries.insert(query);
            self.progress.insert(cid);
        }
    }

    pub fn next(&mut self) -> Option<QueryEvent> {
        loop {
            if let Some(cid) = self.progress.iter().next().cloned() {
                if let Some(mut query) = self.queries.take(&cid) {
                    match query.next() {
                        Some(CidQueryEvent::Query(q)) => {
                            self.queries.insert(query);
                            return Some(q);
                        }
                        Some(CidQueryEvent::Complete(Ok(_have))) => {
                            log::debug!("get ok {}", cid);
                            return Some(QueryEvent::Complete(Query::Get(cid), Ok(())));
                        }
                        Some(CidQueryEvent::Complete(Err(err))) => {
                            log::debug!("get error {}", cid);
                            return Some(QueryEvent::Complete(Query::Get(cid), Err(err)));
                        }
                        None => {
                            self.queries.insert(query);
                        }
                    }
                }
                self.progress.remove(&cid);
            } else {
                return None;
            }
        }
    }
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
