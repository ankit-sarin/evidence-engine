"""PDF acquisition module — OA checking, downloading, and manual list generation."""


def check_oa_status(*args, **kwargs):
    from engine.acquisition.check_oa import check_oa_status as _fn
    return _fn(*args, **kwargs)


def download_papers(*args, **kwargs):
    from engine.acquisition.download import download_papers as _fn
    return _fn(*args, **kwargs)


def generate_manual_list(*args, **kwargs):
    from engine.acquisition.manual_list import generate_manual_list as _fn
    return _fn(*args, **kwargs)


def verify_downloads(*args, **kwargs):
    from engine.acquisition.verify_downloads import verify_downloads as _fn
    return _fn(*args, **kwargs)


__all__ = ["check_oa_status", "download_papers", "generate_manual_list", "verify_downloads"]
