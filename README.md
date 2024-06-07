# IOP JSON Connector

## Summary

Connects to JAUS/IOP Node Manager and translates IOP Messages to/from JSON which can send/receive via Web Socket.

## Install

lone this repository to your preferred destination.

```bash
git clone https://github.com/fkie/iop-json-connector
```

### As ROS package inside ROS environment

If you use it with ROS put this repository into ROS workspace and call

```bash
colcon build --packages-select fkie_iop_json_connector--symlink-install
```

or for ROS1

```bash
roscd && catkin build
```

### As standalone package

Use setup.py to install the code:

```bash
cd iop-json-connector/fkie_iop_json_connector
python3 setup.py install --user --record installed_files.txt
```

The executable **iop-json-connector.py** is now located in `~/.local/bin/`.

**Note:** to remove installed files call

```bash
xargs rm -rf < installed_files.txt
```

## Run

Run **jsidl2json.py** to generate the JSON schemes.

In ROS environment you can do it by

```bash
ros2 run fkie_iop_json_connector ros-iop-json-connector.py
```

or (ROS1)

```bash
rosrun fkie_iop_json_connector ros-iop-json-connector.py
```

otherwise

```bash
python3 ~/.local/bin/iop-json-connector.py
```

## Test

You need a running

- Node Manager from [JausToolsSet][jts] or [ROS/IOP Bridge][ros-iop-bridge].
- IOP robot e.g. from [ROS/IOP Bridge example][ros-iop-bridge-example]

Start the json connector, see **Run** section.

Open **test/websocket.html** in your browser.

You can use _connect_ and _send_ buttons to test the web socket of the running json connector.

[jts]: https://github.com/jaustoolset/jaustoolset
[ros-iop-bridge]: https://github.com/fkie/iop_node_manager
[ros-iop-bridge-example]: https://github.com/fkie/iop_examples/tree/master/fkie_iop_cfg_sim_turtle
