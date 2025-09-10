import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { CheckCircle, AlertTriangle, XCircle, BarChart3, Download, RefreshCw, Trash2, Clock } from "lucide-react";
import { useJobs, useFileDownload } from "@/hooks/useApi";
import { format } from "date-fns";
import { toast } from "@/hooks/use-toast";

const Results = () => {
  const { jobs, isLoading, refetch, deleteJob } = useJobs();
  const { downloadFile } = useFileDownload();

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "completed":
        return <CheckCircle className="h-4 w-4 text-green-500" />;
      case "processing":
        return <Clock className="h-4 w-4 text-blue-500" />;
      case "failed":
        return <XCircle className="h-4 w-4 text-red-500" />;
      default:
        return <AlertTriangle className="h-4 w-4 text-yellow-500" />;
    }
  };

  const getStatusBadge = (status: string) => {
    const variants = {
      completed: "default",
      processing: "secondary",
      failed: "destructive",
    } as const;
    
    return (
      <Badge variant={variants[status as keyof typeof variants] || "secondary"}>
        {status}
      </Badge>
    );
  };

  const handleDownload = async (jobId: string, fileType: 'report' | 'good_data' | 'all_data') => {
    try {
      await downloadFile(jobId, fileType);
      toast({
        title: "Download Started",
        description: `${fileType} for job ${jobId} is downloading.`,
      });
    } catch (error) {
      toast({
        title: "Download Failed",
        description: error instanceof Error ? error.message : "Unknown error occurred",
        variant: "destructive",
      });
    }
  };

  const handleDelete = async (jobId: string) => {
    try {
      await deleteJob(jobId);
      toast({
        title: "Job Deleted",
        description: `Job ${jobId} has been deleted successfully.`,
      });
      refetch();
    } catch (error) {
      toast({
        title: "Delete Failed",
        description: error instanceof Error ? error.message : "Unknown error occurred",
        variant: "destructive",
      });
    }
  };

  const completedJobs = jobs.filter(job => job.status === 'completed');
  const failedJobs = jobs.filter(job => job.status === 'failed');
  const runningJobs = jobs.filter(job => job.status === 'running' || job.status === 'pending');

  const averageQualityScore = completedJobs.length > 0 
    ? Math.round(completedJobs.reduce((sum, job) => sum + (job.result?.success_rate ? parseFloat(job.result.success_rate.replace('%', '')) : 0), 0) / completedJobs.length)
    : 0;

  const totalIssues = jobs.reduce((sum, job) => sum + (job.result?.invalid_records || 0), 0);

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="max-w-6xl mx-auto">
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-foreground mb-2 flex items-center justify-center gap-2">
            <BarChart3 className="h-8 w-8 text-primary" />
            Validation Results
          </h1>
          <p className="text-muted-foreground">
            Review your data validation reports and download processed files
          </p>
        </div>

        <div className="grid md:grid-cols-4 gap-6 mb-8">
          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Total Jobs</CardTitle>
              <BarChart3 className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{isLoading ? "..." : jobs.length}</div>
              <p className="text-xs text-muted-foreground">
                All processing jobs
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Completed</CardTitle>
              <CheckCircle className="h-4 w-4 text-green-500" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{isLoading ? "..." : completedJobs.length}</div>
              <p className="text-xs text-muted-foreground">
                Successfully processed
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Average Quality</CardTitle>
              <CheckCircle className="h-4 w-4 text-muted-foreground" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{isLoading ? "..." : averageQualityScore}%</div>
              <p className="text-xs text-muted-foreground">
                Data quality score
              </p>
            </CardContent>
          </Card>

          <Card>
            <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
              <CardTitle className="text-sm font-medium">Issues Found</CardTitle>
              <AlertTriangle className="h-4 w-4 text-yellow-500" />
            </CardHeader>
            <CardContent>
              <div className="text-2xl font-bold">{isLoading ? "..." : totalIssues}</div>
              <p className="text-xs text-muted-foreground">
                Validation issues
              </p>
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <div className="flex items-center justify-between">
              <div>
                <CardTitle>Processing Jobs</CardTitle>
                <CardDescription>
                  All your data validation and processing jobs
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
                <div className="text-muted-foreground">Loading jobs...</div>
              </div>
            ) : jobs.length === 0 ? (
              <div className="text-center py-8 space-y-4">
                <BarChart3 className="h-12 w-12 text-muted-foreground mx-auto" />
                <div>
                  <p className="text-lg font-medium text-muted-foreground">No jobs found</p>
                  <p className="text-sm text-muted-foreground">
                    Upload some data to see your processing results here.
                  </p>
                </div>
              </div>
            ) : (
              <div className="space-y-4">
                {jobs.map((job) => (
                  <div
                    key={job.job_id}
                    className="flex items-center justify-between p-6 border border-border rounded-lg hover:bg-muted/50 transition-colors"
                  >
                    <div className="flex items-center space-x-4">
                      {getStatusIcon(job.status)}
                      <div>
                        <h3 className="font-medium">{job.filename}</h3>
                        <p className="text-sm text-muted-foreground">
                          {job.result?.total_records?.toLocaleString() || 0} records • {format(new Date(job.created_at), 'MMM dd, yyyy HH:mm')}
                        </p>
                        {job.status === 'completed' && job.result?.valid_records !== undefined && job.result?.invalid_records !== undefined && (
                          <p className="text-xs text-muted-foreground mt-1">
                            Good: {job.result.valid_records.toLocaleString()} • Invalid: {job.result.invalid_records.toLocaleString()}
                          </p>
                        )}
                      </div>
                    </div>
                    <div className="flex items-center space-x-4">
                      <div className="text-right">
                        {job.result?.success_rate && (
                          <div className="text-lg font-semibold">{job.result.success_rate}</div>
                        )}
                        {job.result?.invalid_records !== undefined && job.result.invalid_records > 0 && (
                          <div className="text-sm text-muted-foreground">
                            {job.result.invalid_records} issues
                          </div>
                        )}
                      </div>
                      {getStatusBadge(job.status)}
                      <div className="flex space-x-2">
                        {job.status === 'completed' && (
                          <>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => handleDownload(job.job_id, 'report')}
                            >
                              <Download className="h-4 w-4 mr-2" />
                              Report
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => handleDownload(job.job_id, 'good_data')}
                            >
                              <Download className="h-4 w-4 mr-2" />
                              Good Data
                            </Button>
                            <Button
                              variant="outline"
                              size="sm"
                              onClick={() => handleDownload(job.job_id, 'all_data')}
                            >
                              <Download className="h-4 w-4 mr-2" />
                              All Data
                            </Button>
                          </>
                        )}
                        <Button
                          variant="outline"
                          size="sm"
                          onClick={() => handleDelete(job.job_id)}
                        >
                          <Trash2 className="h-4 w-4 mr-2" />
                          Delete
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

export default Results;