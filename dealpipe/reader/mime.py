def is_excel(file: str) -> bool:
    excel_sigs = [
        ("xlsx", b"\x50\x4B\x05\x06", 2, -22, 4),
        ("xls", b"\x09\x08\x10\x00\x00\x06\x05\x00", 0, 512, 8),  # Saved from Excel
        (
            "xls",
            b"\x09\x08\x10\x00\x00\x06\x05\x00",
            0,
            1536,
            8,
        ),  # Saved from LibreOffice Calc
        (
            "xls",
            b"\x09\x08\x10\x00\x00\x06\x05\x00",
            0,
            2048,
            8,
        ),  # Saved from Excel then saved from Calc
    ]

    for _, sig, whence, offset, size in excel_sigs:
        with open(file, "rb") as f:
            f.seek(offset, whence)
            bytes = f.read(size)

            if bytes == sig:
                return True

    return False


def detect_format(file: str) -> str:
    """A very naive but simple implementation"""
    if file.endswith(".csv"):
        return "csv"
    elif file.endswith(".yaml") or file.endswith(".yml"):
        return "yaml"
    elif ".parquet" in file:
        return "parquet"
    elif is_excel(file):
        return "excel"
    else:
        return "unknown"
