import logging, os

myLogger = logging.getLogger('handler_screen')


def pull(branch, repo_url):
    import git
    path = '/opt/tmp/'
    try:
        files = os.listdir(path)
    except:
        os.makedirs(path)
        files = os.listdir(path)
    if '.git' in files:
        repo = git.Repo('/opt/tmp/')
        if branch != repo.active_branch:
            repo.git.checkout(branch)
        repo.git.pull()
        print("拉完了!")
        myLogger.info('==========All ready pulled!==========')
    else:
        try:
            clone = git.Repo.clone_from(repo_url, to_path='/opt/tmp/', branch=branch, env={
                                            'GIT_SSH_COMMAND': 'ssh -o StrictHostKeyChecking=no'
                                        })
            print("已经克隆啦！")
            myLogger.info('==========All ready cloned!==========')
        except  Exception as e:
            print(e)
            myLogger.info('==========%s=========' % (e))
                                                                                                      
