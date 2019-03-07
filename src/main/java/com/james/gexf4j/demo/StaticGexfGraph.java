package com.james.gexf4j.demo;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Calendar;

import it.uniroma1.dis.wsngroup.gexf4j.core.EdgeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.Gexf;
import it.uniroma1.dis.wsngroup.gexf4j.core.Graph;
import it.uniroma1.dis.wsngroup.gexf4j.core.Mode;
import it.uniroma1.dis.wsngroup.gexf4j.core.Node;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.Attribute;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeClass;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeList;
import it.uniroma1.dis.wsngroup.gexf4j.core.data.AttributeType;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.GexfImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.StaxGraphWriter;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.data.AttributeListImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.viz.ColorImpl;
import it.uniroma1.dis.wsngroup.gexf4j.core.impl.viz.PositionImpl;

public class StaticGexfGraph {
	public static void main(String[] args) throws IOException {
		Gexf gexf = new GexfImpl();
		Calendar date = Calendar.getInstance();

		gexf.getMetadata().setLastModified(date.getTime()).setCreator("james").setDescription("gexf demo");
		gexf.setVisualization(true);

		Graph graph = gexf.getGraph();
		graph.setDefaultEdgeType(EdgeType.UNDIRECTED).setMode(Mode.STATIC);

		AttributeList attrList = new AttributeListImpl(AttributeClass.NODE);
		graph.getAttributeLists().add(attrList);

		Attribute clazz = attrList.createAttribute("modularity_class", AttributeType.INTEGER, "Class");

		Node node1 = graph.createNode("1");
		node1.setLabel("node1").getAttributeValues().addValue(clazz, "1");
		node1.setSize(50).setPosition(new PositionImpl(-200,200,0)).setColor(new ColorImpl(235,81,72));

		Node node2 = graph.createNode("2");
		node2.setLabel("node2").getAttributeValues().addValue(clazz, "2");
		node2.setSize(50).setPosition(new PositionImpl(-300,300,0)).setColor(new ColorImpl(235,81,72));

		node1.connectTo("0", node2).setWeight(5.0f);

		StaxGraphWriter graphWriter = new StaxGraphWriter();
		File f = new File("data/gexf/static_graph_sample.gexf");
		f.createNewFile();
		
		Writer out;
		try {
			out = new FileWriter(f, false);
			graphWriter.writeToStream(gexf, out, "UTF-8");
			System.out.println(f.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
