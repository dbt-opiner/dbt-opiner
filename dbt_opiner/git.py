import shutil
import subprocess
import sys
import tempfile

from loguru import logger


def clone_git_repo_and_checkout_revision(repository, revision):
    """Clones a git repository to a temporary directory and checks out to the revision if provided.
    Args:
        repository: Git repository URL.
        revision: Revision to check out to (tag or commit hash).

    Returns:
        A tempfile.TemporaryDirectory object with the cloned repository.

    To clean up the temporary directory, call the cleanup() method on the returned object after it is no longer needed.
    """
    temp_dir = tempfile.mkdtemp()

    try:
        subprocess.run(
            ["git", "clone", "--quiet", repository, temp_dir],
            check=True,
            stderr=subprocess.PIPE,
        )
        if revision:
            # Revision is optional.
            # If not provided default main branch will be used.
            logger.debug(f"Check out to revision: {revision}")
            subprocess.run(
                ["git", "reset", "--quiet", "--hard", revision],
                check=True,
                stderr=subprocess.PIPE,
                cwd=temp_dir,
            )
        else:
            logger.warning(
                "Repository revision not defined. "
                "Main branch latest commit will be used. "
                "We advise to pin a revision."
            )
    except subprocess.CalledProcessError as e:
        logger.critical(
            f"Could not clone git repository: {repository}. Error: {e.stderr.decode('utf-8')}"
        )
        shutil.rmtree(temp_dir)
        sys.exit(1)
    logger.debug(f"Cloned git repository: {repository} to {temp_dir}")
    return temp_dir
