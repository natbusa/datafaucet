import os
import git


def repo_data(rootdir=None, search_parent_directories=True):
    """
    :param rootdir: the root directory where to look for the repo. (default is current working dir)
    :param search_parent_directories: repo search upwards for a valid .git directory object
    :return: a dictionary with git repository info, if available
    """

    if rootdir is None:
        rootdir = os.getcwd()

    msg = {
        'type': None,
        'committer': '',
        'hash': 0,
        'commit': 0,
        'branch': '',
        # How to get url
        'url': '',
        'name': '',
        # How to get humanable time
        'date': '',
        'clean': False
    }

    try:
        repo = git.Repo(rootdir, search_parent_directories=search_parent_directories)
        (commit, branch) = repo.head.object.name_rev.split(' ')
        msg['type'] = 'git'
        msg['committer'] = repo.head.object.committer.name
        msg['hash'] = commit[:7]
        msg['commit'] = commit
        msg['branch'] = branch
        msg['url'] = repo.remotes.origin.url
        msg['name'] = repo.remotes.origin.url.split('/')[-1]
        msg['date'] = repo.head.object.committed_datetime.isoformat()
        msg['clean'] = len(repo.index.diff(None)) == 0
    except:
        pass

    return msg
