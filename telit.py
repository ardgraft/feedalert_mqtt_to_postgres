import os
import json
import time
import requests
from dotenv import load_dotenv

load_dotenv()

class telitHandler:

    def __init__(self):
        self.assert_status_hook = lambda response, * \
            args, **kwargs: response.raise_for_status()
        self.lookupObject = {}
        self.authObject = {}
        self.authJson = {}
        self.lookupJson = {}
        self.session = {}
        self.authenticated = False
        

    def authenticate(self, env="prod"):
        """ Creates an authenticated connection to Telit using credentials supplied
        
        Returns:
            json: json response including result and data
        """
        MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
        MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
        TELIT_API = os.environ.get("TELIT_API")
        
        if env == "dev":
            self.telitUsername = MQTT_USERNAME
            self.telitPassword = MQTT_PASSWORD
            self.telitAPI = TELIT_API
        else:
            self.telitUsername = MQTT_USERNAME
            self.telitPassword = MQTT_PASSWORD
            self.telitAPI = TELIT_API
        
        
        if self.authenticated == False:

            self.authObject = {
                "auth": {
                    "command": "api.authenticate",
                    "params": {"username": self.telitUsername, "password": self.telitPassword},
                }
            }

            self.authJson = json.dumps(self.authObject)
            self.lookupJson = json.dumps(self.lookupObject)

            with requests.Session() as self.session:

                self.session.hooks["response"] = [self.assert_status_hook]
                self.authResponse = self.session.post(
                    url=self.telitAPI, data=self.authJson)

                if self.authResponse.status_code == 200:

                    self.lookupObject = {}
                    self.lookupObject.update(
                        {
                            "auth": {
                                "sessionId": self.authResponse.json()["auth"]["params"]["sessionId"]
                            }
                        }
                    )
                self.authenticated = True
                return self.session

    def getThingAttr(self, key, attr):
        """ Obtains an attribute of a specific thing

        Args:
            key int: imei of thing
            attr string: attrbiute name

        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()

        self.lookupObject.update(
            {
                "cmd": {
                    "command": "thing.attr.get",
                    "params": {"thingKey": key, "key": attr},
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)

        if lookupResponse.json()["cmd"]["success"] == True:
            thing = lookupResponse.json()["cmd"]["params"]
            return thing["ts"], thing["value"]

        else:

            return (
                lookupResponse.json()["cmd"]["errorMessages"],
                lookupResponse.json()["cmd"]["errorCodes"],
            )

    def setThingAttr(self, key, attr, value):
        """ Sets an attribute of a specific thing

        Args:
            key int: imei of thing
            attr string: attrbiute name
            value string: value to be added to attribute

        Returns:
            json: json command response 
        """

        if self.authenticated == False:
            self.authenticate()

        self.lookupObject.update(
            {
                "cmd": {
                    "command": "thing.attr.set",
                    "params": {
                        "thingKey": key,
                        "key": attr,
                        "value": value
                    },
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)
        return lookupResponse.json()["cmd"]

    def unsetThingAttr(self, key, attr):
        """ Deletes/Unsets an attribute of a specific thing back to a null value

        Args:
            key int: imei of thing
            attr string: attrbiute name

        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()

        self.lookupObject.update(
            {
                "cmd": {
                    "command": "thing.attr.unset",
                    "params": {"thingKey": key, "key": attr},
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)

        return lookupResponse.json()["cmd"]


    def usageThingHistory(self):
        """ Obtains usage history of all things for last 48 hours

        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()

        self.lookupObject.update(
            {
                "1": {
                    "command": "usage.thing.history",
                    "params": {
                        "includeSubOrgs": "false",
                        "showAll": "true",
                        "last": "48h",
                        "series": "day"
                    }
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)

        return lookupResponse.json()["1"]

    def usageConnectionHistory(self):
        """ Obtains connection history of all things/SIMS

        Args:
            key int: imei of thing
            attr string: attrbiute name

        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()

        self.lookupObject.update(
            {
                "1": {
                    "command": "usage.connection.history",
                    "params": {
                        "includeSubOrgs": "false",
                        "showAll": "true",
                        "last": "48h"
                    }
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)

        return lookupResponse.json()["1"]

    def getThing(self, id):
        """ find a thing using a key

        Args:
            id string: search by name or imei

        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()

        if len(id) < 1:
            return False

        self.lookupObject.update(
            {
                "1": {
                    "command": "thing.find",
                    "params": {
                        "key": id
                    }
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)

        return lookupResponse.json()

    def getAllThings(self, offset="0", limit="10"):
        """ Obtains all things

        Args:
            offset int : paging start point
            limit int: number to return in page

        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()

        self.lookupObject.update(
            {
                "1": {
                    "command": "thing.list",
                    "params": {
                        "offset": offset,
                        "limit": limit
                    }
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)

        return lookupResponse.json()

    def deleteThing(self, id):
        """ Delete a thing from Telit 

        Args:
            id int: imei of thing

        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()

        if len(id) < 15:
            return False

        self.lookupObject.update(
            {
                "1": {
                    "command": "thing.delete",
                    "params": {
                        "id": id
                    }
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)

        return lookupResponse.json()

    def isAwake(self, imei):
        """ Pings a thing to see if it is responding

        Args:
            imei int: imei of thing


        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()

        # No imei passed, just fail here
        if imei == 0:
            return False
        
        # Check if the thing is connected
        resp = self.getThing(imei)
        
        if resp['1']['success'] == True:
            if resp['1']['params']['connected'] :
                return True
            else:
                return False
            
    
    def ping(self, imei):
        
        ping = self.getThingAttr(imei, "swd_pingrsp")

        if ping[1][0] == -91006:
            new_ping_val = "1"
        else:
            new_ping_val = str(int(ping[1][0])+1)
            if int(new_ping_val) > 20:
                new_ping_val = "1"
        
        time.sleep(5)
        # Pausing to let the MQTT chatter reduce. More of an issue with 1.05 SWD firmware

        self.setThingAttr(imei, "swd_pingcmd", new_ping_val)
        c=0
        
        while c < 20:
            time.sleep(2)
            ping_response = self.getThingAttr(imei, "swd_pingrsp")
            if ping_response[1][0] == new_ping_val:
                break
            c = c + 1
            

        if ping_response[1][0] == new_ping_val:
            return True
        else:
            return False

    def addThingTags(self, id, tags):
        """ Adds a TAG to a thing

        Args:
            id int: imei of thing
            tag string: tag name

        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()

        if len(id) < 15:
            return False

        self.lookupObject.update(
            {
                "1": {
                    "command": "thing.tag.add",
                    "params": {
                        "thingKey": id,
                        "tags": tags
                    }
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)

        return lookupResponse.json()

    def deleteThingTags(self, id, tags):
        """ removes a tag from a thing

        Args:
            key int: imei of thing
            tags string: tag name to remove

        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()

        if len(id) < 15:
            return False

        self.lookupObject.update(
            {
                "1": {
                    "command": "thing.tag.delete",
                    "params": {
                        "thingKey": id,
                        "tags": tags
                    }
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)
        return lookupResponse.json()

    def findThing(self, id):
        
        if self.authenticated == False:
            self.authenticate()
        
        self.lookupObject.update(
            {
                "1": {
                    "command": "thing.find",
                    "params": {
                        "id": id
                    }
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)
        
        if "false" in lookupResponse.text:
            return False
 
        if "key" in lookupResponse.json()['1']['params']:
            return lookupResponse.json()['1']['params']['key']
        else:
            return False
        
        
    def getThingTags(self, id):
        """ Obtains all TAGS of a specific thing

        Args:
            idint: imei of thing
        Returns:
            json: json response including result and data
        """

        if self.authenticated == False:
            self.authenticate()
        self.lookupObject.update(
            {
                "1": {
                    "command": "thing.find",
                    "params": {
                        "key": id
                    }
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson)

        if "tags" in lookupResponse.json()['1']['params']:
            return lookupResponse.json()['1']['params']['tags']

    def searchForThing(self, id):
        """ Find a thing by wildcard search

        Args:
            id string: partial or complete imei of thing, wildcard supported
        Returns:
            json: json response including result and data
        """
        # Search by partial key, wildcards supported

        if len(id) < 2:  # Ensure we have at least 2 characters to work with
            return ['success : False']

        if self.authenticated == False:
            self.authenticate()

        self.lookupObject.update(
            {
                "1": {
                    "command": "thing.search",
                    "params": {
                        "query": id,
                        "limit": 10,
                        "offset": 0,
                        "show": [
                            "key",
                            "name"
                        ],
                        "sort": "+name"
                    }
                }
            }
        )

        self.lookupJson = json.dumps(self.lookupObject)
        lookupResponse = self.session.post(
            url=self.telitAPI, data=self.lookupJson).json()

        if lookupResponse['1']['success']:
            return lookupResponse['1']['params']['result']
        else:
            return


def main():
    telit = telitHandler()

    # print(telit.getThingAttr("353081091492945","swd_pingcmd"))
    # print(telit.usageThingHistory())
    # print(telit.getThing("353081091492945"))
    # telit.getAllThings(0,10)
    # resp=telit.isAwake("353081091937857")
    #resp = telit.deleteThingTags("353081091937857",['1440'])
    #resp = telit.addThingTags("353081091937857",['periodic-1440'])
    #resp = telit.getThingTags("353081091937857")
    #resp = telit.searchForThing("*simon*")

    # print(resp)


if __name__ == "__main__":
    main()
