"""PDF acquisition module — OA checking, downloading, quality check, and import."""


def check_oa_status(*args, **kwargs):
    from engine.acquisition.check_oa import check_oa_status as _fn
    return _fn(*args, **kwargs)


def download_papers(*args, **kwargs):
    from engine.acquisition.download import download_papers as _fn
    return _fn(*args, **kwargs)


def verify_downloads(*args, **kwargs):
    from engine.acquisition.verify_downloads import verify_downloads as _fn
    return _fn(*args, **kwargs)


def import_dispositions(*args, **kwargs):
    from engine.acquisition.pdf_quality_import import import_dispositions as _fn
    return _fn(*args, **kwargs)


__all__ = [
    "check_oa_status",
    "download_papers",
    "import_dispositions",
    "verify_downloads",
]
