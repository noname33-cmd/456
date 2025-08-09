# repository/mock_repo.py
from core.proxy import load_list_from_file, shuffle_list

class MockRepository:
    def __init__(self, proxies_path: str = "proxies.txt", tokens_path: str = "tokens.txt"):
        self.proxies_path = proxies_path
        self.tokens_path = tokens_path

    def load_proxies_raw(self) -> list[str]:
        return shuffle_list(load_list_from_file(self.proxies_path))

    def load_tokens_raw(self) -> list[str]:
        return shuffle_list(load_list_from_file(self.tokens_path))
