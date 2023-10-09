from dataclasses import dataclass

@dataclass(frozen=True)
class UserData:
    user: str
    rank: str