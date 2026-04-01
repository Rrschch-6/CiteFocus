#!/usr/bin/env python3
"""Run the CiteFocus Flask web app locally."""

from __future__ import annotations

from web.app import app


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)
