import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Shield, AlertTriangle, Download, Eye, Trash2, RefreshCw } from "lucide-react";
import { useQuarantinedItems } from "@/hooks/useApi";
import { format } from "date-fns";

const Quarantine = () => {
  const { data: quarantinedItems = [], isLoading, refetch } = useQuarantinedItems();

  const getSeverityColor = (score: number) => {
    if (score >= 80) return "destructive";
    if (score >= 50) return "secondary";
    return "outline";
  };

  const getSeverityText = (score: number) => {
    if (score >= 80) return "HIGH";
    if (score >= 50) return "MEDIUM";
    return "LOW";
  };

  const highRiskItems = quarantinedItems.filter(item => (item.risk_score || 0) >= 80);
  const totalRecords = quarantinedItems.reduce((sum, item) => sum + (item.records || 0), 0);

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-foreground mb-2 flex items-center justify-center space-x-2">
            <Shield className="h-8 w-8 text-primary" />
            <span>Data Quarantine</span>
          </h1>
          <p className="text-muted-foreground">
            Review and manage flagged data that requires attention
          </p>
        </div>

        <div className="grid md:grid-cols-3 gap-6 mb-8">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Quarantined Items</CardTitle>
              <Shield className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {isLoading ? "..." : quarantinedItems.length}
              </div>
              <p className="text-xs text-muted-foreground">
                Requires review
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">High Risk Items</CardTitle>
              <AlertTriangle className="h-4 w-4 text-destructive" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold text-destructive">
                {isLoading ? "..." : highRiskItems.length}
              </div>
              <p className="text-xs text-muted-foreground">
                Immediate attention needed
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Records</CardTitle>
              <div className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">
                {isLoading ? "..." : totalRecords.toLocaleString()}
              </div>
              <p className="text-xs text-muted-foreground">
                Records in quarantine
              </p>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Quarantined Items</CardTitle>
                <CardDescription>
                  Data records flagged for quality issues or validation failures
                </CardDescription>
              </div>
              <Button
                variant="outline"
                size="sm"
                onClick={() => refetch()}
                disabled={isLoading}
              >
                <RefreshCw className={`h-4 w-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
                Refresh
              </Button>
            </div>
          </CardHeader>
          <CardContent>
            {isLoading ? (
              <div className="flex items-center justify-center py-8">
                <div className="text-muted-foreground">Loading quarantined items...</div>
              </div>
            ) : quarantinedItems.length === 0 ? (
              <div className="text-center py-8 space-y-4">
                <Shield className="h-12 w-12 text-muted-foreground mx-auto" />
                <div>
                  <p className="text-lg font-medium text-muted-foreground">No items in quarantine</p>
                  <p className="text-sm text-muted-foreground">
                    All your data is clean and validated!
                  </p>
                </div>
              </div>
            ) : (
              <div className="space-y-4">
                {quarantinedItems.map((item, index) => (
                  <div
                    key={`${item.id}-${index}`}
                    className="flex items-center justify-between p-6 border border-border rounded-lg bg-card"
                  >
                    <div className="flex items-center space-x-4">
                      <div className="p-2 bg-muted rounded-lg">
                        <AlertTriangle className="h-5 w-5 text-warning" />
                      </div>
                      <div>
                        <h3 className="font-medium text-foreground">
                          {item.filename}
                        </h3>
                        <p className="text-sm text-muted-foreground mb-1">
                          {item.reason || "Validation failure"}
                        </p>
                        <div className="flex items-center space-x-4 text-xs text-muted-foreground">
                          <span>{item.records || 0} records</span>
                          <span>•</span>
                          <span>{format(new Date(item.quarantine_date), 'MMM dd, yyyy HH:mm')}</span>
                          {item.risk_score && (
                            <>
                              <span>•</span>
                              <span>Risk Score: {item.risk_score}%</span>
                            </>
                          )}
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center space-x-4">
                      <Badge variant={getSeverityColor(item.risk_score || 0)}>
                        {getSeverityText(item.risk_score || 0)}
                      </Badge>
                      <div className="flex space-x-2">
                        <Button variant="outline" size="sm">
                          <Eye className="h-4 w-4 mr-2" />
                          Review
                        </Button>
                        <Button variant="outline" size="sm">
                          <Download className="h-4 w-4 mr-2" />
                          Export
                        </Button>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default Quarantine;