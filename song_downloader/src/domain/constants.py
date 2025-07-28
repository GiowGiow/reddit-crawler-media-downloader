from enum import Enum
from typing import List


class AudioDomainType(Enum):
    """Enum representing different types of audio domains."""

    REDDIT = "v.redd.it"
    YOUTUBE = "youtube.com"
    SUNO = "suno.com"
    SUNO_CDN = "cdn1.suno.ai"
    SUNO_SELF = "self.sunoai"
    SOUNDCLOUD = "soundcloud.com"

    @classmethod
    def get_all_domains(cls) -> list[str]:
        """Return a list of all domain values."""
        return [domain.value for domain in cls]

    @classmethod
    def get_suno_domains(cls) -> list[str]:
        """Return a list of Suno-related domains."""
        return [cls.SUNO.value, cls.SUNO_CDN.value, cls.SUNO_SELF.value]
