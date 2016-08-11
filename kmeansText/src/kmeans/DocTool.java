package kmeans;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class DocTool {
	public static String returnNearestCentNum(Map<String, Double> doc,
			Map<String, Map<String, Double>> centers, long dictSize) {
				//最近中心点
				String nearestCendroid = "";
				//最小距离
				double nearestDistance = 0;
				//文档向量长度
				double docLength = 0;
				//中心点向量长度
				double centLength = 0;
				//文档向量与中心点向量内积
				double innerProduct = 0;
				//计算文档向量长度
				Iterator<Entry<String, Double>> docIter = doc.entrySet().iterator();
				while (docIter.hasNext()) {
					Map.Entry<String, Double> entry = (Map.Entry<String, Double>) docIter
							.next();
					docLength += Math.pow(entry.getValue(), 2.0);
				}
				docLength = Math.sqrt(docLength);
				
				//计算文档与所有中心点的余弦距离
				Iterator<Entry<String, Map<String, Double>>> allCendroids = centers
						.entrySet().iterator();
				while (allCendroids.hasNext()) {
					Map.Entry<String, Map<String, Double>> entry = (Entry<String, Map<String, Double>>) allCendroids
							.next();
					for (long i = 0; i < dictSize; i++) {
						if (entry.getValue().containsKey(i)) {
							centLength += Math.pow(entry.getValue().get(i), 2.0);
							if (doc.containsKey(i))
								innerProduct += entry.getValue().get(i) * doc.get(i);
						}
					}	
					//计算余弦距离并更新最近中心点内积
					centLength = Math.sqrt(centLength);
					if (innerProduct / (docLength * centLength) > nearestDistance){
						nearestDistance = innerProduct / (docLength * centLength);
						nearestCendroid = entry.getKey();
					}
					centLength = 0;
					innerProduct = 0;
				}
				
				return nearestCendroid;
	}
	
}
