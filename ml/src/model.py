"""Word2Vec model and SGNS loss for skip-gram training."""
import torch
import torch.nn as nn
import torch.nn.functional as F


class Word2Vec(nn.Module):
    """Two-table sparse embedding model for SGNS training.

    Both tables are initialised with uniform weights in [-0.5/embed_dim, 0.5/embed_dim].
    Only `embeddings_in` is used as the final track representation.
    """

    def __init__(self, vocab_size: int, embed_dim: int):
        super().__init__()
        self.embeddings_in = nn.Embedding(
            num_embeddings=vocab_size, embedding_dim=embed_dim, sparse=True
        )
        self.embeddings_out = nn.Embedding(
            num_embeddings=vocab_size, embedding_dim=embed_dim, sparse=True
        )
        nn.init.uniform_(self.embeddings_in.weight, -0.5 / embed_dim, 0.5 / embed_dim)
        nn.init.uniform_(self.embeddings_out.weight, -0.5 / embed_dim, 0.5 / embed_dim)

    def forward(
        self, center: torch.Tensor, context: torch.Tensor, negatives: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Return positive and negative dot-product scores.

        Args:
            center:    (B,)    center track ids.
            context:   (B,)    positive context track ids.
            negatives: (B, K)  negative sample ids.

        Returns:
            pos_score: (B,)    dot product of center × context embeddings.
            neg_score: (B, K)  dot products of center × each negative embedding.
        """
        ecenter = self.embeddings_in(center)
        econtext = self.embeddings_out(context)
        enegative = self.embeddings_out(negatives)

        pos_score = (ecenter * econtext).sum(dim=1)
        neg_score = torch.bmm(enegative, ecenter.unsqueeze(2)).squeeze(2)

        return pos_score, neg_score

    @property
    def track_embeddings(self) -> torch.Tensor:
        return self.embeddings_in.weight.detach()


def skipgram_loss(pos_score: torch.Tensor, neg_score: torch.Tensor) -> torch.Tensor:
    """SGNS loss: mean of -(log σ(pos) + Σ log σ(−neg)) over the batch."""
    pos_loss = F.logsigmoid(pos_score)
    neg_loss = F.logsigmoid(-neg_score).sum(dim=1)
    return -(pos_loss + neg_loss).mean()
