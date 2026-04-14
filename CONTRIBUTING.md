# Contributing to Cerulean Next

## Getting Started

1. Clone the repo and follow the [Quick Start](README.md#quick-start-development) to get a local dev environment running.
2. Create a branch for your work: `git checkout -b feature/my-feature`
3. Make changes, test locally, commit, and push.
4. Open a Pull Request against `main`.

## Development Setup

```bash
git clone https://github.com/brendan1226/Cerulean-Next.git
cd Cerulean-Next
docker compose up -d --build
docker compose exec web alembic upgrade head
```

- **App**: http://localhost:8000
- **API Docs**: http://localhost:8000/api/docs
- **Flower**: http://localhost:5555

OAuth is bypassed in dev (no `GOOGLE_CLIENT_ID` set), so you can use the app without Google login.

## Code Style

### Python
- **Formatting**: PEP 8, enforced by `black` and `flake8` (both in requirements.txt)
- **Type hints**: Use Python 3.11+ type hints (`str | None` not `Optional[str]`)
- **Imports**: Standard library, then third-party, then local. One blank line between groups.
- **Docstrings**: Required for all router endpoints and Celery tasks. Use the existing format (see any router file).
- **Naming**: snake_case for functions/variables, PascalCase for models/schemas.

### JavaScript (Frontend)
- All frontend code is in `frontend/index.html` — a single-file vanilla JS SPA.
- No build step, no framework, no npm.
- Use `esc()` for ALL server-supplied data interpolated into HTML (XSS prevention).
- Use `window.functionName = async function()` for functions that need to be accessible across script blocks.
- Use `window.api()` for all API calls — it includes the JWT token and handles 401 redirects.
- Use `authDownload()` for all file download links to API endpoints (direct `<a href>` links don't include JWT).

### SQL/Database
- All schema changes go through Alembic migrations.
- Never modify the database directly in production.
- Model changes go in `cerulean/models/__init__.py`.

## Project Structure

```
cerulean/
  api/routers/       # FastAPI route handlers (one file per feature area)
  core/              # Config, database, auth, logging, transform presets
  models/            # SQLAlchemy ORM models (all in __init__.py)
  schemas/           # Pydantic request/response schemas
  tasks/             # Celery background tasks
frontend/
  index.html         # Entire frontend SPA
  vendor/            # Third-party JS (chart.min.js)
docs/                # Documentation
alembic/versions/    # Database migrations
```

## Adding a New Feature

### Backend (API endpoint)

1. Create or extend a router in `cerulean/api/routers/`
2. Add Pydantic schemas in `cerulean/schemas/` if needed
3. Register the router in `cerulean/main.py`
4. If it needs a new database table, add a model to `cerulean/models/__init__.py` and create an Alembic migration

### Backend (Celery task)

1. Add the task function in `cerulean/tasks/`
2. Register the module in `cerulean/tasks/celery_app.py` (`include` list)
3. Add routing in `celery_app.py` (`task_routes`)
4. Ensure the queue is listed in the worker's `--queues` flag in `docker-compose.yml`

### Frontend (new page)

1. Add a nav item in the sidebar HTML (search for `nav-item`)
2. Add to BOTH render maps (there are two — one in each `<script>` block)
3. Add to the breadcrumb labels
4. Add the `window.renderMyPage` function

### Frontend (new tab in existing page)

1. Add a `<button class="tab-btn">` to the tab bar
2. Add the tab name to the `switchTab` function's render map
3. Add the render function

### Database migration

```bash
# After modifying models/__init__.py:
docker compose exec web alembic revision --autogenerate -m "description"
# Review the generated migration, then:
docker compose exec web alembic upgrade head
```

## Route Ordering

**Important**: FastAPI matches routes in definition order. Specific paths like `/templates/import-csv` MUST be defined BEFORE parameterized paths like `/templates/{template_id}`, or the specific path will be matched as a parameter value.

## Testing

```bash
docker compose exec web pytest tests/ -v
docker compose exec web pytest tests/ --cov=cerulean
```

Currently test coverage is limited. When adding new features, please add tests for:
- Happy path API calls
- Error cases (404, 400, 403)
- Edge cases in data processing

## Commit Messages

Use clear, descriptive commit messages. Format:

```
Short summary (what changed)

Longer description if needed:
- Detail 1
- Detail 2

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>
```

Include the Co-Authored-By line if the code was written with AI assistance.

## Pull Request Process

1. Ensure your code compiles: `python3 -m py_compile cerulean/path/to/file.py`
2. Test locally in Docker
3. Update documentation if you added user-facing features
4. Open a PR with a clear description of what changed and why
5. Request review from Brendan

## Deploying to Production

After merging to `main`, deploy to production:

```bash
ssh root@cerulean-next.gallagher-family-hub.com
cd /opt/cerulean
git pull
docker compose -f docker-compose.yml -f docker-compose.prod.yml exec web alembic upgrade head
docker compose -f docker-compose.yml -f docker-compose.prod.yml restart web worker worker-push
```

The nginx resolver config handles DNS re-resolution, so you should NOT need to restart nginx after web restarts.

## Questions?

- Check the [Architecture Guide](docs/ARCHITECTURE.md) for codebase orientation
- Check the [API Reference](docs/API-REFERENCE.md) for endpoint details
- Submit a Suggestion in the app (type: Discussion) for design questions
- Contact Brendan for access or permissions
