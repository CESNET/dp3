from urllib.parse import quote_plus

CESNET_APP_REPOS = (
    "CESNET/Amfora",
    "CESNET/ADiCT",
    "CESNET/NERD2",
)

GITHUB_CODE_SEARCH_URL = "https://github.com/search?q={query}&type=code"


def _repo_query(repos: tuple[str, ...]) -> str:
    return " OR ".join(f"repo:{repo}" for repo in repos)


def _github_code_search_link(label: str, query: str) -> str:
    encoded_query = quote_plus(query)
    return f"[{label}]({GITHUB_CODE_SEARCH_URL.format(query=encoded_query)})"


def define_env(env):
    @env.macro
    def query_cesnet_apps(label: str, query: str) -> str:
        full_query = f"{_repo_query(CESNET_APP_REPOS)} {query}"
        return _github_code_search_link(label, full_query)

    @env.macro
    def registrar_usage(label: str, registrar_call: str) -> str:
        return query_cesnet_apps(f"`{label}`", f"registrar.{registrar_call}")
