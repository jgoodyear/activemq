/*
* Copyright 2006 The Apache Software Foundation or its licensors, as
* applicable.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#include "activemq/command/RemoveSubscriptionInfo.hpp"

using namespace apache::activemq::command;

/*
 *
 *  Marshalling code for Open Wire Format for RemoveSubscriptionInfo
 *
 *
 *  NOTE!: This file is autogenerated - do not modify!
 *         if you need to make a change, please see the Groovy scripts in the
 *         activemq-core module
 *
 */
RemoveSubscriptionInfo::RemoveSubscriptionInfo()
{
    this->connectionId = NULL ;
    this->subcriptionName = NULL ;
    this->clientId = NULL ;
}

RemoveSubscriptionInfo::~RemoveSubscriptionInfo()
{
}

unsigned char RemoveSubscriptionInfo::getDataStructureType()
{
    return RemoveSubscriptionInfo::TYPE ; 
}

        
p<ConnectionId> RemoveSubscriptionInfo::getConnectionId()
{
    return connectionId ;
}

void RemoveSubscriptionInfo::setConnectionId(p<ConnectionId> connectionId)
{
    this->connectionId = connectionId ;
}

        
p<string> RemoveSubscriptionInfo::getSubcriptionName()
{
    return subcriptionName ;
}

void RemoveSubscriptionInfo::setSubcriptionName(p<string> subcriptionName)
{
    this->subcriptionName = subcriptionName ;
}

        
p<string> RemoveSubscriptionInfo::getClientId()
{
    return clientId ;
}

void RemoveSubscriptionInfo::setClientId(p<string> clientId)
{
    this->clientId = clientId ;
}

int RemoveSubscriptionInfo::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> writer) throw (IOException)
{
    int size = 0 ;

    size += marshaller->marshalInt(commandId, mode, writer) ;
    size += marshaller->marshalBoolean(responseRequired, mode, writer) ; 
    size += marshaller->marshalObject(connectionId, mode, writer) ; 
    size += marshaller->marshalString(subcriptionName, mode, writer) ; 
    size += marshaller->marshalString(clientId, mode, writer) ; 
    return size ;
}

void RemoveSubscriptionInfo::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> reader) throw (IOException)
{
    commandId = marshaller->unmarshalInt(mode, reader) ;
    responseRequired = marshaller->unmarshalBoolean(mode, reader) ; 
    connectionId = p_cast<ConnectionId>(marshaller->unmarshalObject(mode, reader)) ; 
    subcriptionName = p_cast<string>(marshaller->unmarshalString(mode, reader)) ; 
    clientId = p_cast<string>(marshaller->unmarshalString(mode, reader)) ; 
}
