use filecoin_proofs_api::{
    seal, Commitment, ProverId, RegisteredSealProof, SectorId, Ticket, UnpaddedByteIndex,
    UnpaddedBytesAmount,
};
use libipld::error::Result;
use libp2p::core::PeerId;
use std::fs::File;
use std::path::Path;
use tempfile::NamedTempFile;

fn peer_id_to_prover_id(peer_id: &PeerId) -> ProverId {
    let mut buf = [0u8; 32];
    let offset = peer_id.as_bytes().len() - 32;
    buf.copy_from_slice(&peer_id.as_bytes()[offset..]);
    buf
}

pub struct Seal {
    pub registered_proof: RegisteredSealProof,
    pub comm_r: Commitment,
    pub comm_d: Commitment,
    pub peer_id: PeerId,
    pub sector_id: u64,
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
            peer_id_to_prover_id(&self.peer_id),
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
            peer_id_to_prover_id(&self.peer_id),
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
        mut data: &[u8],
        cache_dir: &Path,
        replica_path: &Path,
        peer_id: PeerId,
        sector_id: u64,
        ticket: [u8; 32],
        seed: [u8; 32],
    ) -> Result<Self> {
        let prover_id = peer_id_to_prover_id(&peer_id);
        let mut pieces_path = NamedTempFile::new()?;
        let len = data.len();
        let (info, _size) = seal::add_piece(
            registered_proof,
            &mut data,
            &mut pieces_path,
            UnpaddedBytesAmount(len as u64),
            &[],
        )?;
        let piece_infos = &[info];
        let pp1 = seal::seal_pre_commit_phase1(
            registered_proof.into(),
            cache_dir,
            pieces_path.path(),
            replica_path,
            prover_id,
            SectorId::from(sector_id),
            Ticket::from(ticket),
            piece_infos,
        )?;
        let pp2 = seal::seal_pre_commit_phase2(pp1, cache_dir, replica_path)?;
        let p1 = seal::seal_commit_phase1(
            cache_dir,
            replica_path,
            prover_id,
            SectorId::from(sector_id),
            Ticket::from(ticket),
            Ticket::from(seed),
            pp2,
            piece_infos,
        )?;
        let comm_d = p1.comm_d;
        let comm_r = p1.comm_r;
        let p2 = seal::seal_commit_phase2(p1, prover_id, SectorId::from(sector_id))?;
        Ok(Seal {
            registered_proof,
            peer_id,
            sector_id,
            ticket,
            seed,
            comm_r,
            comm_d,
            proof: p2.proof,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_seal_verify_unseal() {
        env_logger::try_init().ok();
        let cache_dir = tempdir().unwrap();
        let replica = NamedTempFile::new().unwrap();
        let peer_id = PeerId::random();
        let sector_id = 42;
        let ticket = [23u8; 32];
        let seed = [44u8; 32];
        let data = vec![42u8; 2032];

        let seal = Seal::seal(
            RegisteredSealProof::StackedDrg2KiBV1,
            &data,
            cache_dir.path(),
            replica.path(),
            peer_id.clone(),
            sector_id,
            ticket,
            seed,
        )
        .unwrap();
        assert!(seal.verify().unwrap());
        let bytes = seal.unseal(cache_dir.path(), replica.path(), 0, data.len() as _).unwrap();
        assert_eq!(bytes, data);
    }
}
