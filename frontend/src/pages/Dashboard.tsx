/**
 * Main Dashboard for Data Ingestion Pipeline
 */

import React, { useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Badge } from '@/components/ui/badge';
import { Progress } from '@/components/ui/progress';
import { Alert, AlertDescription } from '@/components/ui/alert';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { 
  Upload, 
  FileText, 
  Database, 
  Download, 
  RefreshCw, 
  Trash2,
  CheckCircle,
  XCircle,
  Clock,
  Play
} from 'lucide-react';

import { useFileUpload, useJobs, useJobStatus, useFileDownload, useApiHealth } from '@/hooks/useApi';
import { JobStatus } from '@/lib/api';

const Dashboard: React.FC = () => {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [processingAction, setProcessingAction] = useState<'validate' | 'kafka' | 'pipeline'>('validate');
  const [activeJobId, setActiveJobId] = useState<string | null>(null);

  const { uploadFile, isUploading, uploadError, clearError } = useFileUpload();
  const { jobs, isLoading, error: jobsError, refetch, deleteJob } = useJobs();
  const { status: activeJobStatus } = useJobStatus(activeJobId);
  const { downloadFile, isDownloading } = useFileDownload();
  const { isHealthy, healthData } = useApiHealth();

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      setSelectedFile(file);
      clearError();
    }
  };

  const handleUpload = async () => {
    if (!selectedFile) return;

    const jobId = await uploadFile(selectedFile, processingAction);
    if (jobId) {
      setActiveJobId(jobId);
      refetch(); // Refresh jobs list
    }
  };

  const handleDownload = async (jobId: string, fileType: 'report' | 'good_data' | 'all_data') => {
    await downloadFile(jobId, fileType);
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'completed':
        return <CheckCircle className="h-4 w-4 text-green-500" />;
      case 'failed':
        return <XCircle className="h-4 w-4 text-red-500" />;
      case 'running':
        return <RefreshCw className="h-4 w-4 text-blue-500 animate-spin" />;
      default:
        return <Clock className="h-4 w-4 text-yellow-500" />;
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed':
        return 'bg-green-100 text-green-800';
      case 'failed':
        return 'bg-red-100 text-red-800';
      case 'running':
        return 'bg-blue-100 text-blue-800';
      default:
        return 'bg-yellow-100 text-yellow-800';
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Data Ingestion Pipeline</h1>
            <p className="text-gray-600 mt-2">Intelligent data validation and processing platform</p>
          </div>
          <div className="flex items-center space-x-2">
            {isHealthy === null ? (
              <Badge variant="secondary">Checking...</Badge>
            ) : isHealthy ? (
              <Badge className="bg-green-100 text-green-800">API Healthy</Badge>
            ) : (
              <Badge variant="destructive">API Offline</Badge>
            )}
          </div>
        </div>

        <Tabs defaultValue="upload" className="space-y-6">
          <TabsList className="grid w-full grid-cols-3">
            <TabsTrigger value="upload">Upload & Process</TabsTrigger>
            <TabsTrigger value="jobs">Job History</TabsTrigger>
            <TabsTrigger value="monitor">Active Monitoring</TabsTrigger>
          </TabsList>

          {/* Upload Tab */}
          <TabsContent value="upload" className="space-y-6">
            <Card>
              <CardHeader>
                <CardTitle className="flex items-center space-x-2">
                  <Upload className="h-5 w-5" />
                  <span>File Upload</span>
                </CardTitle>
                <CardDescription>
                  Upload your data file for validation, Kafka processing, or full pipeline execution
                </CardDescription>
              </CardHeader>
              <CardContent className="space-y-6">
                {/* File Selection */}
                <div>
                  <label className="block text-sm font-medium mb-2">Select File</label>
                  <input
                    type="file"
                    accept=".csv,.xlsx,.json,.parquet"
                    onChange={handleFileSelect}
                    className="block w-full text-sm text-gray-500
                             file:mr-4 file:py-2 file:px-4
                             file:rounded-md file:border-0
                             file:text-sm file:font-medium
                             file:bg-blue-50 file:text-blue-700
                             hover:file:bg-blue-100"
                  />
                  {selectedFile && (
                    <p className="text-sm text-gray-600 mt-2">
                      Selected: {selectedFile.name} ({(selectedFile.size / 1024 / 1024).toFixed(2)} MB)
                    </p>
                  )}
                </div>

                {/* Processing Action */}
                <div>
                  <label className="block text-sm font-medium mb-2">Processing Action</label>
                  <div className="grid grid-cols-3 gap-4">
                    <Card 
                      className={`cursor-pointer transition-colors ${
                        processingAction === 'validate' ? 'ring-2 ring-blue-500 bg-blue-50' : ''
                      }`}
                      onClick={() => setProcessingAction('validate')}
                    >
                      <CardContent className="p-4 text-center">
                        <FileText className="h-8 w-8 mx-auto mb-2 text-blue-600" />
                        <h3 className="font-medium">Validate Only</h3>
                        <p className="text-sm text-gray-600">Run intelligent validation and generate reports</p>
                      </CardContent>
                    </Card>

                    <Card 
                      className={`cursor-pointer transition-colors ${
                        processingAction === 'kafka' ? 'ring-2 ring-blue-500 bg-blue-50' : ''
                      }`}
                      onClick={() => setProcessingAction('kafka')}
                    >
                      <CardContent className="p-4 text-center">
                        <Database className="h-8 w-8 mx-auto mb-2 text-green-600" />
                        <h3 className="font-medium">Send to Kafka</h3>
                        <p className="text-sm text-gray-600">Validate and stream to Kafka topic</p>
                      </CardContent>
                    </Card>

                    <Card 
                      className={`cursor-pointer transition-colors ${
                        processingAction === 'pipeline' ? 'ring-2 ring-blue-500 bg-blue-50' : ''
                      }`}
                      onClick={() => setProcessingAction('pipeline')}
                    >
                      <CardContent className="p-4 text-center">
                        <Play className="h-8 w-8 mx-auto mb-2 text-purple-600" />
                        <h3 className="font-medium">Full Pipeline</h3>
                        <p className="text-sm text-gray-600">Complete end-to-end processing</p>
                      </CardContent>
                    </Card>
                  </div>
                </div>

                {/* Error Display */}
                {uploadError && (
                  <Alert variant="destructive">
                    <XCircle className="h-4 w-4" />
                    <AlertDescription>{uploadError}</AlertDescription>
                  </Alert>
                )}

                {/* Upload Button */}
                <Button 
                  onClick={handleUpload}
                  disabled={!selectedFile || isUploading}
                  className="w-full"
                  size="lg"
                >
                  {isUploading ? (
                    <>
                      <RefreshCw className="mr-2 h-4 w-4 animate-spin" />
                      Uploading...
                    </>
                  ) : (
                    <>
                      <Upload className="mr-2 h-4 w-4" />
                      Start Processing
                    </>
                  )}
                </Button>
              </CardContent>
            </Card>
          </TabsContent>

          {/* Jobs Tab */}
          <TabsContent value="jobs" className="space-y-6">
            <div className="flex items-center justify-between">
              <h2 className="text-xl font-semibold">Job History</h2>
              <Button onClick={refetch} variant="outline" size="sm">
                <RefreshCw className="mr-2 h-4 w-4" />
                Refresh
              </Button>
            </div>

            {jobsError && (
              <Alert variant="destructive">
                <XCircle className="h-4 w-4" />
                <AlertDescription>{jobsError}</AlertDescription>
              </Alert>
            )}

            <div className="grid gap-4">
              {jobs.map((job) => (
                <Card key={job.job_id}>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center space-x-4">
                        {getStatusIcon(job.status)}
                        <div>
                          <h3 className="font-medium">{job.filename || 'Unknown File'}</h3>
                          <p className="text-sm text-gray-600">{job.message}</p>
                          <div className="flex items-center space-x-2 mt-1">
                            <Badge className={getStatusColor(job.status)}>
                              {job.status}
                            </Badge>
                            <Badge variant="outline">{job.action}</Badge>
                            <span className="text-xs text-gray-500">
                              {new Date(job.created_at).toLocaleString()}
                            </span>
                          </div>
                        </div>
                      </div>

                      <div className="flex items-center space-x-2">
                        {job.status === 'running' && (
                          <div className="w-32">
                            <Progress value={job.progress} className="h-2" />
                            <span className="text-xs text-gray-500">{job.progress}%</span>
                          </div>
                        )}

                        {job.status === 'completed' && (
                          <div className="flex space-x-1">
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => handleDownload(job.job_id, 'report')}
                            >
                              <Download className="h-4 w-4 mr-1" />
                              Report
                            </Button>
                            <Button
                              size="sm"
                              variant="outline"
                              onClick={() => handleDownload(job.job_id, 'good_data')}
                            >
                              <Download className="h-4 w-4 mr-1" />
                              Data
                            </Button>
                          </div>
                        )}

                        <Button
                          size="sm"
                          variant="outline"
                          onClick={() => deleteJob(job.job_id)}
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>

                    {job.status === 'completed' && job.result && (
                      <div className="mt-4 p-4 bg-gray-50 rounded-lg">
                        <h4 className="font-medium mb-2">Results Summary</h4>
                        <div className="grid grid-cols-4 gap-4 text-sm">
                          <div>
                            <span className="text-gray-600">Total Records:</span>
                            <span className="font-medium ml-1">{job.result.total_records}</span>
                          </div>
                          <div>
                            <span className="text-gray-600">Valid:</span>
                            <span className="font-medium ml-1 text-green-600">{job.result.valid_records}</span>
                          </div>
                          <div>
                            <span className="text-gray-600">Invalid:</span>
                            <span className="font-medium ml-1 text-red-600">{job.result.invalid_records}</span>
                          </div>
                          <div>
                            <span className="text-gray-600">Success Rate:</span>
                            <span className="font-medium ml-1">{job.result.success_rate}</span>
                          </div>
                        </div>
                      </div>
                    )}

                    {job.status === 'failed' && job.error && (
                      <div className="mt-4 p-4 bg-red-50 rounded-lg">
                        <h4 className="font-medium text-red-800 mb-2">Error Details</h4>
                        <p className="text-sm text-red-700">{job.error}</p>
                      </div>
                    )}
                  </CardContent>
                </Card>
              ))}

              {jobs.length === 0 && !isLoading && (
                <Card>
                  <CardContent className="p-8 text-center">
                    <FileText className="h-12 w-12 mx-auto text-gray-400 mb-4" />
                    <h3 className="text-lg font-medium text-gray-600 mb-2">No jobs yet</h3>
                    <p className="text-gray-500">Upload a file to start processing</p>
                  </CardContent>
                </Card>
              )}
            </div>
          </TabsContent>

          {/* Monitor Tab */}
          <TabsContent value="monitor" className="space-y-6">
            <h2 className="text-xl font-semibold">Active Job Monitoring</h2>

            {activeJobStatus ? (
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center space-x-2">
                    {getStatusIcon(activeJobStatus.status)}
                    <span>Job: {activeJobStatus.job_id}</span>
                  </CardTitle>
                  <CardDescription>{activeJobStatus.filename}</CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium">Progress</span>
                    <span className="text-sm text-gray-600">{activeJobStatus.progress}%</span>
                  </div>
                  <Progress value={activeJobStatus.progress} className="h-3" />
                  <p className="text-sm text-gray-600">{activeJobStatus.message}</p>

                  <div className="flex items-center space-x-2">
                    <Badge className={getStatusColor(activeJobStatus.status)}>
                      {activeJobStatus.status}
                    </Badge>
                    <Badge variant="outline">{activeJobStatus.action}</Badge>
                  </div>

                  {activeJobStatus.status === 'completed' && activeJobStatus.result && (
                    <div className="p-4 bg-green-50 rounded-lg">
                      <h4 className="font-medium text-green-800 mb-2">Job Completed Successfully!</h4>
                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                          <span className="text-gray-600">Processing Duration:</span>
                          <span className="font-medium ml-1">{activeJobStatus.result.statistics?.processing_duration || 'N/A'}</span>
                        </div>
                        <div>
                          <span className="text-gray-600">Success Rate:</span>
                          <span className="font-medium ml-1">{activeJobStatus.result.success_rate}</span>
                        </div>
                      </div>
                    </div>
                  )}

                  {activeJobStatus.status === 'failed' && (
                    <Alert variant="destructive">
                      <XCircle className="h-4 w-4" />
                      <AlertDescription>{activeJobStatus.error}</AlertDescription>
                    </Alert>
                  )}
                </CardContent>
              </Card>
            ) : (
              <Card>
                <CardContent className="p-8 text-center">
                  <Clock className="h-12 w-12 mx-auto text-gray-400 mb-4" />
                  <h3 className="text-lg font-medium text-gray-600 mb-2">No active job</h3>
                  <p className="text-gray-500">Upload a file to start monitoring</p>
                </CardContent>
              </Card>
            )}
          </TabsContent>
        </Tabs>
      </div>
    </div>
  );
};

export default Dashboard;
