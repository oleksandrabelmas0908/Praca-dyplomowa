VERSION = "0.1.0"


def service_info(name: str) -> dict[str, str]:
    return {"service": name, "version": VERSION}
