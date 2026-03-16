from ..wrapper import lib


class EncryptionMixin:

    def to_prdx_encrypted(self, path: str, key: str, algo: str = "aes256"):
        """
        Write the DataFrame to an encrypted .prdx file.

        Args:
            path (str): Output file path.
            key (str): Encryption key string.
            algo (str): Algorithm — "aes256" (default) or "chacha20".

        Returns:
            int: Bytes written (positive on success).
        """
        if not hasattr(lib, 'pardox_write_prdx_encrypted'):
            raise NotImplementedError("pardox_write_prdx_encrypted not found in Core.")
        result = lib.pardox_write_prdx_encrypted(
            self._ptr,
            path.encode('utf-8'),
            key.encode('utf-8'),
            algo.encode('utf-8')
        )
        if result < 0:
            raise RuntimeError(f"to_prdx_encrypted failed with code: {result}")
        return int(result)
