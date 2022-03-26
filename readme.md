# Set up
1. git clone this repo into a folder on the computer. CD into folder

2. Set up an alias on computer. In terminal type:
`
sudo nano ~/.zshrc
`
add a line with the path to the script
`
alias box='<PATH_TO_SCRIPT>'
`
save changes
`
source ~/.zshrc
`
3. Edit config.cfg file. M
  Make sure the database folder lines up to the HOROS db folder and all the paths to the studies match up
4. In terminal type:
   `
   box initdb
   `
   after completed:
   `
   ls
   `
   you should see a db file has been created in the location you specified in the config file (usually in script folder)


# Run
1. Fill databse by running 
`
box fill
`

2. transfer all by running
`
box transfer
`
3. transfer one study by
`
box transfer --study=<study name>
`







## Conda Stuff
- Create: * conda env create --name dicom-parse --file=requirements.yml*
conda list --export > package-list.txt


https://medium.com/macoclock/how-to-create-delete-update-bash-profile-in-macos-5f99999ed1e7


