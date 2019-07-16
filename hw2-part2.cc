/**
 * @file PageRankVertex.cc
 * @author  Songjie Niu, Shimin Chen
 * @version 0.1
 *
 * @section LICENSE 
 * 
 * Copyright 2016 Shimin Chen (chensm@ict.ac.cn) and
 * Songjie Niu (niusongjie@ict.ac.cn)
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
 * 
 * @section DESCRIPTION
 * 
 * This file implements the PageRank algorithm using graphlite API.
 *
 */

#include <stdio.h>
#include <string.h>
#include <math.h>

#include "GraphLite.h"

#include <map>
#include <vector>
#include <iostream>
#include <algorithm>

#define VERTEX_CLASS_NAME(name) DirectedTriangleCount##name

typedef struct Result {
    int through, cycle;//in = out = through
} Result;

class VERTEX_CLASS_NAME(InputFormatter): public InputFormatter {
public:
    int64_t getVertexNum() {
        unsigned long long n;
        sscanf(m_ptotal_vertex_line, "%lld", &n);
        m_total_vertex= n;
        return m_total_vertex;
    }
    int64_t getEdgeNum() {
        unsigned long long n;
        sscanf(m_ptotal_edge_line, "%lld", &n);
        m_total_edge= n;
        return m_total_edge;
    }
    int getVertexValueSize() {
        m_n_value_size = sizeof(Result);
        return m_n_value_size;
    }
    int getEdgeValueSize() {
        m_e_value_size = sizeof(double);
        return m_e_value_size;
    }
    int getMessageValueSize() {
        m_m_value_size = sizeof(int64_t);
        return m_m_value_size;
    }
    void loadGraph() {
        unsigned long long last_vertex;
        unsigned long long from;
        unsigned long long to;
        double weight = 0;
        
        Result value;
        int outdegree = 0;
        
        const char *line= getEdgeLine();

        // Note: modify this if an edge weight is to be read
        //       modify the 'weight' variable

        sscanf(line, "%lld %lld", &from, &to);
        addEdge(from, to, &weight);

        last_vertex = from;
        ++outdegree;
        for (int64_t i = 1; i < m_total_edge; ++i) {
            line= getEdgeLine();

            // Note: modify this if an edge weight is to be read
            //       modify the 'weight' variable

            sscanf(line, "%lld %lld", &from, &to);
            if (last_vertex != from) {
                addVertex(last_vertex, &value, outdegree);
                last_vertex = from;
                outdegree = 1;
            } else {
                ++outdegree;
            }
            addEdge(from, to, &weight);
        }
        addVertex(last_vertex, &value, outdegree);
    }
};


class VERTEX_CLASS_NAME(OutputFormatter): public OutputFormatter {
public:
    void writeResult() {
    	int64_t vid;
		Result value;
		char s[1024];
        
		for (ResultIterator r_iter; ! r_iter.done(); r_iter.next() ) {
            r_iter.getIdValue(vid, &value);
            int n = sprintf(s, "in: %d\nout: %d\nthrough: %d\ncycle: %d\n", value.through, value.through, value.through, value.cycle);
            writeNextResLine(s, n);
        }
    }
};

// An aggregator that records a double value tom compute sum
class VERTEX_CLASS_NAME(Aggregator): public Aggregator<Result> {
public:
    void init() {
        memset(&m_global, 0, sizeof(Result));
        memset(&m_local, 0, sizeof(Result));
    }
    void* getGlobal() {
        return &m_global;
    }
    void setGlobal(const void* p) {
        memmove(&m_global, p, sizeof(Result));
    }
    void* getLocal() {
        return &m_local;
    }
    void merge(const void* p) {
        m_global.through += ((Result*)p)->through;
        m_global.cycle += ((Result*)p)->cycle;
    }
    void accumulate(const void* p) {
        m_local.through += ((Result*)p)->through;
        m_local.cycle += ((Result*)p)->cycle;
    }
};

class VERTEX_CLASS_NAME(): public Vertex <Result, double, int64_t> {
public:
    map<int64_t, vector<int64_t>> In_Father_Map; // in from neighbor
	map<int64_t, vector<int64_t>> Out_Son_Map; // out to neighbor

    void compute(MessageIterator* pmsgs) {
    	   
        int64_t val = getVertexId(); //vertex's id
        
        if (getSuperstep() == 0) {
			vector<int64_t>& Out_Son = Out_Son_Map[val];
			OutEdgeIterator it = getOutEdgeIterator();
			for ( ; ! it.done(); it.next() ) {
				Out_Son.push_back(it.target()); //all the son of vertex is saved in map
			}
            sendMessageToAllNeighbors(val);
        }
        
        else if(getSuperstep() == 1) {
            vector<int64_t>& In_Neighbor = In_Father_Map[val];//
            for ( ; ! pmsgs->done(); pmsgs->next() ) {
                int64_t msg = pmsgs->getValue();
                In_Neighbor.push_back(msg);//all the father of vertex is saved in map
                sendMessageToAllNeighbors(msg);//send father to son
			}
		}

        else if (getSuperstep() == 2) {
			Result result = {0,0};
			vector<int64_t>& In_Neighbor = In_Father_Map[val];
			vector<int64_t>& Out_Neighbor = Out_Son_Map[val];
           	for ( ; ! pmsgs->done(); pmsgs->next() ) {//
                int64_t msg = pmsgs->getValue();
                for (vector<int64_t>::iterator iter1 = In_Neighbor.begin(); iter1 != In_Neighbor.end(); ++iter1){//through count
					if(msg == *iter1)	result.through++;
                }
                for (vector<int64_t>::iterator iter2 = Out_Neighbor.begin(); iter2 != Out_Neighbor.end(); ++iter2){//cycle count
                	if(msg == *iter2)	result.cycle++;
				}
            }
			accumulateAggr(0, &result);
        }

        else if (getSuperstep() == 3){
			* mutableValue() = *(Result*)getAggrGlobal(0);//modify value of vertex to result
            voteToHalt();//turn to inactive
        }
    }
};

class VERTEX_CLASS_NAME(Graph): public Graph {
public:
    VERTEX_CLASS_NAME(Aggregator)* aggregator;

public:
    // argv[0]: PageRankVertex.so
    // argv[1]: <input path>
    // argv[2]: <output path>
    void init(int argc, char* argv[]) {

        setNumHosts(5);
        setHost(0, "localhost", 1411);
        setHost(1, "localhost", 1421);
        setHost(2, "localhost", 1431);
        setHost(3, "localhost", 1441);
        setHost(4, "localhost", 1451);

        if (argc < 3) {
           printf ("Usage: %s <input path> <output path>\n", argv[0]);
           exit(1);
        }

        m_pin_path = argv[1];
        m_pout_path = argv[2];

        aggregator = new VERTEX_CLASS_NAME(Aggregator)[1];
        regNumAggr(1);
        regAggr(0, &aggregator[0]);
    }

    void term() {
        delete[] aggregator;
    }
};

/* STOP: do not change the code below. */
extern "C" Graph* create_graph() {
    Graph* pgraph = new VERTEX_CLASS_NAME(Graph);

    pgraph->m_pin_formatter = new VERTEX_CLASS_NAME(InputFormatter);
    pgraph->m_pout_formatter = new VERTEX_CLASS_NAME(OutputFormatter);
    pgraph->m_pver_base = new VERTEX_CLASS_NAME();

    return pgraph;
}

extern "C" void destroy_graph(Graph* pobject) {
    delete ( VERTEX_CLASS_NAME()* )(pobject->m_pver_base);
    delete ( VERTEX_CLASS_NAME(OutputFormatter)* )(pobject->m_pout_formatter);
    delete ( VERTEX_CLASS_NAME(InputFormatter)* )(pobject->m_pin_formatter);
    delete ( VERTEX_CLASS_NAME(Graph)* )pobject;
}
