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
#include "activemq/command/SubscriptionInfo.hpp"

using namespace apache::activemq::command;

/*
 *
 *  Marshalling code for Open Wire Format for SubscriptionInfo
 *
 *
 *  NOTE!: This file is autogenerated - do not modify!
 *         if you need to make a change, please see the Groovy scripts in the
 *         activemq-core module
 *
 */
SubscriptionInfo::SubscriptionInfo()
{
    this->clientId = NULL ;
    this->destination = NULL ;
    this->selector = NULL ;
    this->subcriptionName = NULL ;
}

SubscriptionInfo::~SubscriptionInfo()
{
}

unsigned char SubscriptionInfo::getDataStructureType()
{
    return SubscriptionInfo::TYPE ; 
}

        
p<string> SubscriptionInfo::getClientId()
{
    return clientId ;
}

void SubscriptionInfo::setClientId(p<string> clientId)
{
    this->clientId = clientId ;
}

        
p<ActiveMQDestination> SubscriptionInfo::getDestination()
{
    return destination ;
}

void SubscriptionInfo::setDestination(p<ActiveMQDestination> destination)
{
    this->destination = destination ;
}

        
p<string> SubscriptionInfo::getSelector()
{
    return selector ;
}

void SubscriptionInfo::setSelector(p<string> selector)
{
    this->selector = selector ;
}

        
p<string> SubscriptionInfo::getSubcriptionName()
{
    return subcriptionName ;
}

void SubscriptionInfo::setSubcriptionName(p<string> subcriptionName)
{
    this->subcriptionName = subcriptionName ;
}

int SubscriptionInfo::marshal(p<IMarshaller> marshaller, int mode, p<IOutputStream> writer) throw (IOException)
{
    int size = 0 ;

    size += marshaller->marshalInt(commandId, mode, writer) ;
    size += marshaller->marshalBoolean(responseRequired, mode, writer) ; 
    size += marshaller->marshalString(clientId, mode, writer) ; 
    size += marshaller->marshalObject(destination, mode, writer) ; 
    size += marshaller->marshalString(selector, mode, writer) ; 
    size += marshaller->marshalString(subcriptionName, mode, writer) ; 
    return size ;
}

void SubscriptionInfo::unmarshal(p<IMarshaller> marshaller, int mode, p<IInputStream> reader) throw (IOException)
{
    commandId = marshaller->unmarshalInt(mode, reader) ;
    responseRequired = marshaller->unmarshalBoolean(mode, reader) ; 
    clientId = p_cast<string>(marshaller->unmarshalString(mode, reader)) ; 
    destination = p_cast<ActiveMQDestination>(marshaller->unmarshalObject(mode, reader)) ; 
    selector = p_cast<string>(marshaller->unmarshalString(mode, reader)) ; 
    subcriptionName = p_cast<string>(marshaller->unmarshalString(mode, reader)) ; 
}
