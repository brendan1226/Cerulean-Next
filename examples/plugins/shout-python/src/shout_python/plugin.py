"""Reference Cerulean plugin — pure Python, transforms every value to
uppercase with a trailing exclamation mark. Copy this file as your
starting point."""


def setup(ctx):
    """Called once at app / worker startup. Register every hook the
    plugin exposes. The ``key`` must match what the manifest declares
    under ``extension_points`` — mismatches fail the load loudly."""

    def shout(value: str, config: dict) -> str:
        return (value or "").upper() + "!"

    ctx.register_transform("shout", shout)
