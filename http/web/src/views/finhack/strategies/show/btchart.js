 export function drawChart(date_list,sr,br,er) {
  var ctx = document.getElementById('bt-chart').getContext('2d');
  var statisticsChart = new Chart(ctx, {
  	type: 'line',
  	data: {
  		labels: date_list,
  		datasets: [ {
  			label: "策略收益",
  			borderColor: '#f3545d',
  			pointRadius: 0,
  			backgroundColor: 'transparent',
  			legendColor: '#f3545d',
  			fill: true,
  			borderWidth: 2,
  			data:sr
  		},  {
  			label: "基准收益(中证1000)",
  			borderColor: '#177dff',
  			pointRadius: 0,
  			backgroundColor: 'transparent',
  			legendColor: '#177dff',
  			fill: true,
  			borderWidth: 2,
  			data: br
  		},
  		{
  			label: "超额收益",
  			borderColor: '#F39C12',
  			pointRadius:0,
  			backgroundColor: 'transparent',
  			legendColor: '#F39C12',
  			fill: true,
  			borderWidth: 2,
  			data: er
  		}]
  	},
  	options : {
  		responsive: true, 
  		maintainAspectRatio: false,
  		legend: {
      labels: {
        usePointStyle: false, // 设置为false，使用实心颜色填充legend
      },
  			display: true
  		},
  		tooltips: {
  			bodySpacing: 4,
  			mode:"nearest",
  			intersect: 0,
  			position:"nearest",
  			xPadding:10,
  			yPadding:10,
  			caretPadding:10,
  			         callbacks: {
               label: function (tooltipItem, data) {
                   var type = data.datasets[tooltipItem.datasetIndex].label;
                   var value = data.datasets[tooltipItem.datasetIndex].data[tooltipItem.index];
  					return type+" : "+(value*100).toFixed(2)+"%"
               }
           }
  		},
  		layout:{
  			padding:{left:5,right:5,top:15,bottom:15}
  		},
  		scales: {
            xAxes: [{
                ticks: {
                    beginAtZero: true
                }
            }],
            yAxes: [{
                ticks: {
                    beginAtZero: true
                },
                scaleLabel: {
                    display: true,
                    labelString: 'Value'
                },
                gridLines: {
                    zeroLineWidth: 1,
                    zeroLineColor: 'rgba(0, 0, 0, 0.25)',
                    color: 'rgba(0, 0, 0, 0.1)'
                }
            }]
  		}, 
  		legendCallback: function(chart) { 
  			var text = []; 
  			text.push('<ul class="' + chart.id + '-legend html-legend">'); 
  			for (var i = 0; i < chart.data.datasets.length; i++) { 
  				text.push('<li><span style="background-color:' + chart.data.datasets[i].legendColor + '"></span>'); 
  				if (chart.data.datasets[i].label) { 
  					text.push(chart.data.datasets[i].label); 
  				} 
  				text.push('</li>'); 
  			} 
  			text.push('</ul>'); 
  			return text.join(''); 
  		}  
  	}
  });
  

}