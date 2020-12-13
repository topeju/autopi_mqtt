# autopi_mqtt

MQTT returner, cache and management service for [AutoPi](https://www.autopi.io/). This can be used to replace the default `cloud` returner, and in fact also calls the cloud returner so that all data is sent to both MQTT and the AutoPi cloud.

You will obviously need an MQTT broker set up somewhere -- setting one up is outside the scope of this documentation (see, e.g., [Mosquitto](https://mosquitto.org/)). This service does not need to be continuously connected to the MQTT broker, but will queue data in the Redis cache in AutoPi and publishes it when it is able to connect.

## Installation

You will need to add the three files as custom code (Advanced -> Custom Code) in AutoPi:

* `my_mqtt_cache.py` as a utility, add `paho.mqtt==1.5.0` in the Requirements field
* `my_mqtt.py` as a returner
* `my_mqtt_manager.py` as a service

Edit the MQTT settings in `my_mqtt_cache.py` to connect to your MQTT broker.

## Services configuration

The AutoPi services need to be customised to use the MQTT returner. Go through the following services under Advanced -> Services to customise them as documented below.

If at any point the `my_mqtt` returner does not show up, you will need to restart the AutoPi minion. You can do this e.g. via the Terminal in the upper right hand corner with the command `minionutil.request_restart immediately=true`, and wait for a minute or so for the service to restart.

### `acc_manager`

On the Hooks tab, create a new Hook named `my_mqtt` with:
* Name: `my_mqtt`
* Type: returner
* Function: `my_mqtt.returner_data`
* Enabled: checked
* Args: `[]`
* Kwargs: `{}`

On the Workers tab, open the `xyz_logger` worker, and in its workflow, change the returner from `cloud` to `my_mqtt`.

### `event_reactor`

On the Hooks tab, create a new Hook named `my_mqtt` with:
* Name: `my_mqtt`
* Type: returner
* Function: `my_mqtt.returner_event`
* Enabled: checked
* Args: `[]`
* Kwargs: `{}`

On the Reactors tab, create a new reactor with:
* Name: `mqtt_upload_on_event`
* Regex: `^system/power/(sleep|hibernate|reboot)`
* Description: Upload data to MQTT and the cloud
* Order: 10
* Keyword resolve: not checked
* Enabled: checked
* Do not add new conditions
* Add new action with:
  * Handler: module
  * Args: `[ "my_mqtt.upload" ]`
  * kwargs: `{}`
  * Converter: none
  * Filter: none
  * Returner: none

This reactor triggers AutoPi to upload the data before the system goes to sleep.

Open the `cache_events` reactor, and in its action, change the returner from `cloud` to `my_mqtt`.

### `obd_manager`

On the Hooks tab, create a new Hook named `my_mqtt` with:
* Name: `my_mqtt`
* Type: returner
* Function: `my_mqtt.returner_data`
* Enabled: checked
* Args: `[]`
* Kwargs: `{}`

Go to Car Explorer -> Loggers, and edit all of your loggers to use the `my_mqtt` returner. You will need to click the logger to edit it, then press the plus button next to "Advanced (click to show)" to be able to edit the returner.

### `tracking_manager`

On the Hooks tab, create a new Hook named `my_mqtt` with:
* Name: `my_mqtt`
* Type: returner
* Function: `my_mqtt.returner_data`
* Enabled: checked
* Args: `[ "track" ]`
* Kwargs: `{}`

On the Workers tab, open the `poll_logger` worker, and in its workflow, change the returner from `cloud` to `my_mqtt`.

## Operation

This setup publishes all items you have configured through the services above both to the AutoPi cloud and to the MQTT broker you have configured. The published items will use the `topic_root` configured in `my_mqtt_cache.py` as the first part of the publish topic, and the rest will be the type of item that is logged. For example, through the `tracking_manager`, you will get items logged in the `topic_root/track.pos` topic.

## Notes

The code here is largely based on [autopi-io/autopi-core](https://github.com/autopi-io/autopi-core), particularly its [cloud_manager.py](https://github.com/autopi-io/autopi-core/blob/master/src/salt/base/ext/_engines/cloud_manager.py), [cloud_cache.py](https://github.com/autopi-io/autopi-core/blob/master/src/salt/base/ext/_utils/cloud_cache.py), and [cloud_returner.py](https://github.com/autopi-io/autopi-core/blob/master/src/salt/base/ext/_returners/cloud_returner.py).
