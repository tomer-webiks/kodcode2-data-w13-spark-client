# Big Data dataset
The followig dataset is stored on Google Drive because 250MB, too big to store in GIT.
You can download it to experiment with streaming

# To run Jupyter Notebook from WSL
From the venv environment:
- pip install ipykernel --force-reinstall
- pip install pyspark
Make sure the selected kernel is the /bin/python from the /venv folder

# Make sure you are the user cloning and saving files from VSC
Make sure the the cloned folder is owned by the user and not the root, e.g. when you clone the repo, be as the 'tomer' user.

# SSH connection (for multiple repos / separate accounts / becauses of security), follow these steps
Check if your ssh key exists (by default is id_rsa)
- ls -al ~/.ssh
Create an ssh key using rsa encryption
- ssh-keygen -t rsa -b 4096 -C "tomer_sag@webiks.com"  -->  Use all defaults, don't add a passphrase
Check the ssh-agent is running
- eval "$(ssh-agent -s)"  -->  Agent pid 386381
Add the new ssh key
- ssh-add ~/.ssh/id_rsa  -->  Identity added: /home/tomer/.ssh/id_rsa (tomer_sag@webiks.com)
Copy the public key to add to Github
- cat ~/.ssh/id_rsa.pub  -->  displays the key 'ssh-rsa ...............................'
Copy the public key
Go to Github to the SSH Keys section (https://github.com/settings/keys)
New SSH Key
Paste the public key and save (leave 'Authentication Key' selected)
Reset the remote
- git remote set-url origin git@github.com:tomer-webiks/kodcode2-data-w13-spark-client.git
Push as normal