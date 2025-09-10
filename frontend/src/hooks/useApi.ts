/**
 * React hooks for API integration
 */

import { useState, useEffect, useCallback } from 'react';
import { apiClient, JobStatus, UploadResponse } from '../lib/api';

// Hook for uploading files and tracking progress
export function useFileUpload() {
  const [isUploading, setIsUploading] = useState(false);
  const [uploadError, setUploadError] = useState<string | null>(null);

  const uploadFile = useCallback(async (
    file: File, 
    action: 'validate' | 'kafka' | 'pipeline' = 'validate'
  ): Promise<string | null> => {
    setIsUploading(true);
    setUploadError(null);

    try {
      const response = await apiClient.uploadFile(file, action);
      return response.job_id;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Upload failed';
      setUploadError(errorMessage);
      return null;
    } finally {
      setIsUploading(false);
    }
  }, []);

  return {
    uploadFile,
    isUploading,
    uploadError,
    clearError: () => setUploadError(null)
  };
}

// Hook for tracking job status with polling
export function useJobStatus(jobId: string | null, autoRefresh = true) {
  const [status, setStatus] = useState<JobStatus | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchStatus = useCallback(async () => {
    if (!jobId) return;

    setIsLoading(true);
    setError(null);

    try {
      const jobStatus = await apiClient.getJobStatus(jobId);
      setStatus(jobStatus);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to fetch status';
      setError(errorMessage);
    } finally {
      setIsLoading(false);
    }
  }, [jobId]);

  useEffect(() => {
    if (!jobId || !autoRefresh) return;

    fetchStatus();

    // Poll every 2 seconds if job is running
    const interval = setInterval(() => {
      if (status?.status === 'running' || status?.status === 'pending') {
        fetchStatus();
      }
    }, 2000);

    return () => clearInterval(interval);
  }, [jobId, autoRefresh, fetchStatus, status?.status]);

  return {
    status,
    isLoading,
    error,
    refetch: fetchStatus
  };
}

// Hook for managing multiple jobs
export function useJobs() {
  const [jobs, setJobs] = useState<JobStatus[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchJobs = useCallback(async () => {
    setIsLoading(true);
    setError(null);

    try {
      const response = await apiClient.listJobs();
      setJobs(response.jobs.sort((a, b) => 
        new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
      ));
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to fetch jobs';
      setError(errorMessage);
    } finally {
      setIsLoading(false);
    }
  }, []);

  const deleteJob = useCallback(async (jobId: string) => {
    try {
      await apiClient.deleteJob(jobId);
      setJobs(prev => prev.filter(job => job.job_id !== jobId));
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to delete job';
      setError(errorMessage);
      throw error;
    }
  }, []);

  useEffect(() => {
    fetchJobs();
  }, [fetchJobs]);

  return {
    jobs,
    isLoading,
    error,
    refetch: fetchJobs,
    deleteJob
  };
}

// Hook for downloading files
export function useFileDownload() {
  const [isDownloading, setIsDownloading] = useState(false);
  const [downloadError, setDownloadError] = useState<string | null>(null);

  const downloadFile = useCallback(async (
    jobId: string, 
    fileType: 'report' | 'good_data' | 'all_data'
  ) => {
    setIsDownloading(true);
    setDownloadError(null);

    try {
      const blob = await apiClient.downloadFile(jobId, fileType);
      
      // Create download link
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      
      // Set filename based on type
      const extensions: Record<string, string> = {
        report: '.html',
        good_data: '.csv',
        all_data: '.csv'
      };
      
      link.download = `${fileType}_${jobId}${extensions[fileType] || ''}`;
      document.body.appendChild(link);
      link.click();
      
      // Cleanup
      window.URL.revokeObjectURL(url);
      document.body.removeChild(link);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Download failed';
      setDownloadError(errorMessage);
    } finally {
      setIsDownloading(false);
    }
  }, []);

  return {
    downloadFile,
    isDownloading,
    downloadError,
    clearError: () => setDownloadError(null)
  };
}

// Hook for API health check
export function useApiHealth() {
  const [isHealthy, setIsHealthy] = useState<boolean | null>(null);
  const [healthData, setHealthData] = useState<any>(null);

  useEffect(() => {
    const checkHealth = async () => {
      try {
        const health = await apiClient.healthCheck();
        setHealthData(health);
        setIsHealthy(true);
      } catch {
        setIsHealthy(false);
      }
    };

    checkHealth();
    const interval = setInterval(checkHealth, 30000); // Check every 30 seconds

    return () => clearInterval(interval);
  }, []);

  return { isHealthy, healthData };
}

// Hook for quarantined items
export function useQuarantinedItems() {
  const [data, setData] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchQuarantinedItems = useCallback(async () => {
    setIsLoading(true);
    setError(null);

    try {
      const response = await apiClient.getQuarantinedItems();
      setData(response.quarantined_items || []);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to fetch quarantined items';
      setError(errorMessage);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchQuarantinedItems();
  }, [fetchQuarantinedItems]);

  return {
    data,
    isLoading,
    error,
    refetch: fetchQuarantinedItems
  };
}

// Hook for system statistics
export function useStatistics() {
  const [data, setData] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchStatistics = useCallback(async () => {
    setIsLoading(true);
    setError(null);

    try {
      const stats = await apiClient.getStatistics();
      setData(stats);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Failed to fetch statistics';
      setError(errorMessage);
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchStatistics();
    const interval = setInterval(fetchStatistics, 10000); // Refresh every 10 seconds

    return () => clearInterval(interval);
  }, [fetchStatistics]);

  return {
    data,
    isLoading,
    error,
    refetch: fetchStatistics
  };
}
