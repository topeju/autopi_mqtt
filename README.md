# autopi_mqtt

***This project is archived: I don't personally use AutoPi anymore, and have no idea if there have been changes in AutoPi in the last three years that would affect this project (or make it unnecessary).***

MQTT returner, cache and management service for [AutoPi](https://www.autopi.io/). As of AutoPi version 2021.03.10, AutoPi can call multiple returners, so the support to publish data both to MQTT and the AutoPi cloud has been removed. The same AutoPi version also adds support for MQTT natively, but I have not been able to get it to work yet, and in any case the configuration available is somewhat limited.

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

### `my_mqtt_manager`

On the Hooks tab, create one or three (the last two aren't really necessary at the moment, see below) new hooks:
 * Upload:
   * Name: `upload`
   * Type: handler
   * Function: `my_mqtt_manager.upload_handler`
   * Enabled: checked
   * Args: `[]`
   * Kwargs: `{}`
 * Sleep:
   * Name: `sleep`
   * Type: handler
   * Function: `my_mqtt_manager.sleep_handler`
   * Enabled: checked
   * Args: `[]`
   * Kwargs: `{}`
 * Wake:
   * Name: `wake`
   * Type: handler
   * Function: `my_mqtt_manager.wake_handler`
   * Enabled: checked
   * Args: `[]`
   * Kwargs: `{}`

On the Workers tab, create a new Worker with:
 * Name: `upload`
 * Start delay: 5 seconds (exact value shouldn't matter, probably, I just try to keep this from conflicting with the engine startup)
 * Interval: 1 sec (or as desired)
 * Loop: -1
 * Order: 1
 * Enabled: checked
 * Transactional: not checked
 * Kill on success: not checked
 * Suppress exceptions: checked
 * Auto start: checked
 * Add new Workflow with:
   * Handler: `upload`
   * Args: `[]`
   * kwargs: `{}`

On the Reactors tab, you may create new Reactors to handle engine start and stop scenarios. However, these have not been implemented in the `my_mqtt_cache` yet, so these will not do anything beyond log messages claiming the MQTT service is waking up or going to sleep:

Wake on engine start:
 * Name: `wake_on_engine_start`
 * Regex: `^vehicle/engine/running`
 * Enabled: checked
 * No conditions
 * Add new Action:
   * Handler: `wake`
   * Args: `[]`
   * kwargs: `{}`

Sleep on engine stop:
 * Name: `sleep_on_engine_stop`
 * Regex: `^vehicle/engine/stopped`
 * Enabled: checked
 * No conditions
 * Add new Action:
   * Handler: `sleep`
   * Args: `[]`
   * kwargs: `{}`

### `acc_manager`

On the Hooks tab, create a new Hook named `my_mqtt` with:
* Name: `my_mqtt`
* Type: returner
* Function: `my_mqtt.returner_data`
* Enabled: checked
* Args: `[]`
* Kwargs: `{}`

On the Workers tab, open the `xyz_logger` worker, and in its workflow, add the `my_mqtt` returner to the list of returners.

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
* Description: Upload data to MQTT
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

Open the `cache_events` reactor, and in its action, add the `my_mqtt` returner to the list of returners.

### `obd_manager`

On the Hooks tab, create a new Hook named `my_mqtt` with:
* Name: `my_mqtt`
* Type: returner
* Function: `my_mqtt.returner_data`
* Enabled: checked
* Args: `[]`
* Kwargs: `{}`

Go to Car Explorer -> Loggers, and edit all of your loggers to add the `my_mqtt` returner. You will need to click the logger to edit it, then press the plus button next to "Advanced (click to show)" to be able to edit the returner.

### `tracking_manager`

On the Hooks tab, create a new Hook named `my_mqtt` with:
* Name: `my_mqtt`
* Type: returner
* Function: `my_mqtt.returner_data`
* Enabled: checked
* Args: `[ "track" ]`
* Kwargs: `{}`

On the Workers tab, open the `poll_logger` worker, and in its workflow, add the `my_mqtt` returner to the list of returners.

## Operation

This setup publishes all items you have configured through the services above to the MQTT broker you have configured. The published items will use the `topic_root` configured in `my_mqtt_cache.py` as the first part of the publish topic, and the rest will be the type of item that is logged. For example, through the `tracking_manager`, you will get items logged in the `topic_root/track.pos` topic.

## Notes

The code here is largely based on [autopi-io/autopi-core](https://github.com/autopi-io/autopi-core), particularly its [cloud_manager.py](https://github.com/autopi-io/autopi-core/blob/master/src/salt/base/ext/_engines/cloud_manager.py), [cloud_cache.py](https://github.com/autopi-io/autopi-core/blob/master/src/salt/base/ext/_utils/cloud_cache.py), and [cloud_returner.py](https://github.com/autopi-io/autopi-core/blob/master/src/salt/base/ext/_returners/cloud_returner.py). This returner also uses the Redis cache (database 2, whereas the AutoPi cloud uses database 1) to store the data until it can be published to the MQTT broker.

I have sometimes observed AutoPi having trouble loading the custom code, but don't know why this happens. It seems to be resolved by restarting the Minion (see instructions above under "Services configuration").
