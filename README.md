# kitbox
A docker container image for debug

## Build

```sh
$ clone https://github.com/ktmrmshk/kitbox.git
$ cd kitbox
$ docker build -t ghcr.io/ktmrmshk/kitabox_arm64:latest .

$ export CR_PAT=YOUR_TOKEN
$ echo $CR_PAT | docker login ghcr.io -u ktmrmshk --password-stdin

$ dokcer push ghcr.io/ktmrmshk/kitabox_arm64:latest
```

