#!/usr/bin/env python3

import sys
from fkie_iop_json_connector import main

if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        import traceback
        print(traceback.format_exc())
        print("Error while initialize ROS-Node: %s" % (err), file=sys.stderr)
