use filecoin_proofs_api::{
    post, seal, Commitment, PrivateReplicaInfo, ProverId, PublicReplicaInfo, RegisteredSealProof,
    SectorId, Ticket, UnpaddedByteIndex, UnpaddedBytesAmount, RegisteredPoStProof,
};
use libipld::error::Result;
use libp2p::core::PeerId;
use std::collections::BTreeMap;
use std::fs::File;
use std::path::Path;

fn peer_id_to_prover_id(peer_id: &PeerId) -> ProverId {
    let mut buf = [0u8; 32];
    let offset = peer_id.as_bytes().len() - 32;
    buf.copy_from_slice(&peer_id.as_bytes()[offset..]);
    buf
}

pub struct Seal {
    pub registered_proof: RegisteredSealProof,
    pub prover_id: [u8; 32],
    pub sector_id: u64,
    pub comm_d: Commitment,
    pub comm_r: Commitment,
    /// Prover randomness
    pub ticket: [u8; 32],
    /// Verifier randomness
    pub seed: [u8; 32],
    pub proof: Vec<u8>,
}

impl Seal {
    pub fn verify(&self) -> Result<bool> {
        Ok(seal::verify_seal(
            self.registered_proof,
            self.comm_r,
            self.comm_d,
            ProverId::from(self.prover_id),
            SectorId::from(self.sector_id),
            Ticket::from(self.ticket),
            Ticket::from(self.seed),
            &self.proof,
        )?)
    }

    pub fn unseal(
        &self,
        cache_dir: &Path,
        replica_path: &Path,
        offset: u64,
        num_bytes: u64,
    ) -> Result<Vec<u8>> {
        let replica = File::open(replica_path)?;
        let mut buf = Vec::with_capacity(num_bytes as usize);
        seal::unseal_range(
            self.registered_proof,
            cache_dir,
            replica,
            &mut buf,
            ProverId::from(self.prover_id),
            SectorId::from(self.sector_id),
            self.comm_d,
            Ticket::from(self.ticket),
            UnpaddedByteIndex(offset),
            UnpaddedBytesAmount(num_bytes),
        )?;
        Ok(buf)
    }

    pub fn seal(
        registered_proof: RegisteredSealProof,
        prover_id: [u8; 32],
        sector_id: u64,
        cache_dir: &Path,
        pieces_path: &Path,
        replica_path: &Path,
        mut data: &[u8],
        ticket: [u8; 32],
        seed: [u8; 32],
    ) -> Result<Self> {
        let pieces = File::create(pieces_path)?;
        let len = data.len();
        let (info, _size) = seal::add_piece(
            registered_proof,
            &mut data,
            pieces,
            UnpaddedBytesAmount(len as u64),
            &[],
        ).unwrap();
        let pp1 = seal::seal_pre_commit_phase1(
            registered_proof.into(),
            cache_dir,
            pieces_path,
            replica_path,
            prover_id,
            SectorId::from(sector_id),
            Ticket::from(ticket),
            &[info],
        ).unwrap();
        let pp2 = seal::seal_pre_commit_phase2(pp1, cache_dir, replica_path).unwrap();
        let comm_d = pp2.comm_d;
        let comm_r = pp2.comm_r;
        /*let p1 = seal::seal_commit_phase1(
            cache_dir,
            replica_path,
            prover_id,
            SectorId::from(sector_id),
            Ticket::from(ticket),
            Ticket::from(seed),
            pp2,
            piece_infos,
        )?;*/
        //let p2 = seal::seal_commit_phase2(p1, prover_id, SectorId::from(sector_id))?;
        Ok(Seal {
            registered_proof,
            prover_id,
            sector_id,
            ticket,
            seed,
            comm_r,
            comm_d,
            proof: Default::default(), //p2.proof,
        })
    }
}

pub struct PoSt {
    pub registered_proof: RegisteredPoStProof,
    pub prover_id: [u8; 32],
    pub sector_id: u64,
    pub comm_r: Commitment,
    pub challenge: [u8; 32],
    pub proof: Vec<u8>,
}

impl PoSt {
    pub fn new(
        registered_proof: RegisteredPoStProof,
        prover_id: [u8; 32],
        sector_id: u64,
        cache_dir: &Path,
        replica_path: &Path,
        comm_r: Commitment,
        challenge: [u8; 32],
    ) -> Result<Self> {
        let priv_replica_info = PrivateReplicaInfo::new(
            registered_proof,
            comm_r,
            cache_dir.into(),
            replica_path.into(),
        );
        let mut replicas = BTreeMap::new();
        replicas.insert(SectorId::from(sector_id), priv_replica_info);
        let mut proofs = post::generate_window_post(&challenge, &replicas, prover_id)?;
        let proof = proofs.pop().unwrap().1;
        Ok(Self {
            registered_proof,
            prover_id,
            sector_id,
            comm_r,
            challenge,
            proof,
        })
    }

    pub fn verify(&self) -> Result<bool> {
        let pub_replica_info = PublicReplicaInfo::new(
            self.registered_proof,
            self.comm_r,
        );
        let mut replicas = BTreeMap::new();
        replicas.insert(SectorId::from(self.sector_id), pub_replica_info);
        Ok(post::verify_window_post(
            &self.challenge,
            &[(self.registered_proof, &self.proof)],
            &replicas,
            ProverId::from(self.prover_id),
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /*#[test]
    #[ignore]
    fn test_seal_verify_unseal() {
        env_logger::try_init().ok();
        let cache_dir = tempdir().unwrap();
        let replica = NamedTempFile::new().unwrap();
        let data = vec![42u8; 2032];

        let seal = Seal::seal(
            RegisteredSealProof::StackedDrg2KiBV1,
            PeerId::random(),
            42, // sector_id
            cache_dir.path(),
            replica.path(),
            &data,
            [23u8; 32], // ticket
            [44u8; 32], // seed
        )
        .unwrap();
        assert!(seal.verify().unwrap());
        let bytes = seal
            .unseal(cache_dir.path(), replica.path(), 0, data.len() as _)
            .unwrap();
        assert_eq!(bytes, data);
    }*/

    #[test]
    fn test_post_no_seal() {
        env_logger::try_init().ok();
        //let cache_dir = tempdir().unwrap();
        //let replica = NamedTempFile::new().unwrap();
        let cache_dir_path = &Path::new("/tmp/cache");
        let pieces_path = &Path::new("/tmp/pieces");
        let replica_path = &Path::new("/tmp/replica");

        let data = vec![90u8; 2032];
        let prover_id = [103u8; 32];
        let sector_id  = 42;

        /*let seal = Seal::seal(
            RegisteredSealProof::StackedDrg2KiBV1,
            prover_id,
            sector_id,
            cache_dir_path,
            pieces_path,
            replica_path,
            &data,
            [23u8; 32], // ticket
            [44u8; 32], // seed
        )
        .unwrap();*/

        let comm_r = seal::fauxrep(
            RegisteredSealProof::StackedDrg2KiBV1,
            cache_dir_path,
            replica_path,
        ).unwrap();

        let post = PoSt::new(
            RegisteredPoStProof::StackedDrgWindow2KiBV1,
            prover_id,
            sector_id,
            cache_dir_path,
            replica_path,
            comm_r,
            [93u8; 32], // challenge
        ).unwrap();
        assert!(post.verify().unwrap());
    }
}
