efs_id=fs-059384bad21bbbb39
cd ~
mkdir efs
sudo apt-get update
sudo apt-get -y install git binutils
git clone https://github.com/aws/efs-utils
cd efs-utils
./build-deb.sh
sudo apt-get -y install ./build/amazon-efs-utils*deb
sudo mount -t efs $efs_id ~/efs
cd ~/efs
sudo chmod go+rw .