import React, { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Upload as UploadIcon, FileText, Database, Loader2 } from "lucide-react";
import { useToast } from "@/hooks/use-toast";
import { apiClient, JobStatus } from "@/lib/api";
import { useNavigate } from "react-router-dom";

const Upload = () => {
  const [dragActive, setDragActive] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const { toast } = useToast();
  const navigate = useNavigate();
  const fileInputRef = React.useRef<HTMLInputElement>(null);

  const handleDrag = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    if (e.type === "dragenter" || e.type === "dragover") {
      setDragActive(true);
    } else if (e.type === "dragleave") {
      setDragActive(false);
    }
  };

  const handleFileUpload = async (file: File) => {
    if (uploading) return;
    
    setUploading(true);
    setUploadProgress(0);
    
    try {
      toast({
        title: "Uploading file...",
        description: `Processing ${file.name}`,
      });

      // Upload file to backend
      const response = await apiClient.uploadFile(file, 'validate');
      
      toast({
        title: "File uploaded successfully!",
        description: `Job ID: ${response.job_id}`,
      });

      // Poll for job completion
      await apiClient.pollJobStatus(response.job_id, (status: JobStatus) => {
        setUploadProgress(status.progress);
        if (status.status === 'running') {
          toast({
            title: "Processing...",
            description: status.message,
          });
        }
      });

      toast({
        title: "Validation completed!",
        description: "Your data has been processed successfully",
      });

      // Navigate to results page
      navigate('/results');
      
    } catch (error) {
      console.error('Upload error:', error);
      toast({
        title: "Upload failed",
        description: error instanceof Error ? error.message : "An error occurred",
        variant: "destructive",
      });
    } finally {
      setUploading(false);
      setUploadProgress(0);
    }
  };

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    e.stopPropagation();
    setDragActive(false);
    
    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) {
      handleFileUpload(files[0]);
    }
  };

  const handleFileInput = (e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    if (files.length > 0) {
      // Only process the first file for now
      handleFileUpload(files[0]);
    }
    // Reset the input so the same file can be selected again
    e.target.value = '';
  };

  const handleButtonClick = () => {
    if (fileInputRef.current && !uploading) {
      fileInputRef.current.click();
    }
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="max-w-4xl mx-auto">
        <div className="text-center mb-8">
          <h1 className="text-3xl font-bold text-foreground mb-2">Upload Dataset</h1>
          <p className="text-muted-foreground">
            Upload your data files for validation and quality analysis
          </p>
        </div>

        <Card className="mb-8">
          <CardContent className="p-8">
            <div
              className={`border-2 border-dashed rounded-lg p-12 text-center transition-colors ${
                dragActive
                  ? "border-primary bg-primary/5"
                  : uploading
                  ? "border-muted bg-muted/50"
                  : "border-border hover:border-primary/50"
              }`}
              onDragEnter={handleDrag}
              onDragLeave={handleDrag}
              onDragOver={handleDrag}
              onDrop={handleDrop}
            >
              {uploading ? (
                <>
                  <Loader2 className="h-16 w-16 text-primary mx-auto mb-4 animate-spin" />
                  <h3 className="text-xl font-semibold mb-2">Processing your file...</h3>
                  <p className="text-muted-foreground mb-4">
                    {uploadProgress}% complete
                  </p>
                  <div className="w-full bg-secondary rounded-full h-2 mb-4">
                    <div 
                      className="bg-primary h-2 rounded-full transition-all duration-300"
                      style={{ width: `${uploadProgress}%` }}
                    ></div>
                  </div>
                </>
              ) : (
                <>
                  <UploadIcon className="h-16 w-16 text-muted-foreground mx-auto mb-4" />
                  <h3 className="text-xl font-semibold mb-2">Drag and drop your files here</h3>
                  <p className="text-muted-foreground mb-4">
                    or click to browse your computer
                  </p>
                  <input
                    ref={fileInputRef}
                    type="file"
                    accept=".csv,.json,.xlsx,.xls"
                    onChange={handleFileInput}
                    className="hidden"
                    disabled={uploading}
                  />
                  <Button 
                    variant="default" 
                    className="cursor-pointer" 
                    disabled={uploading}
                    onClick={handleButtonClick}
                  >
                    Choose Files
                  </Button>
                  <p className="text-sm text-muted-foreground mt-4">
                    Supported formats: CSV, JSON, Excel
                  </p>
                </>
              )}
            </div>
          </CardContent>
        </Card>

        <div className="grid md:grid-cols-2 gap-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <FileText className="h-5 w-5" />
                <span>File Requirements</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="space-y-2 text-sm">
                <li>• Maximum file size: 100MB</li>
                <li>• Supported formats: CSV, JSON, Excel</li>
                <li>• UTF-8 encoding recommended</li>
                <li>• Headers should be in the first row</li>
              </ul>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <Database className="h-5 w-5" />
                <span>Validation Process</span>
              </CardTitle>
            </CardHeader>
            <CardContent>
              <ul className="space-y-2 text-sm">
                <li>• Data quality assessment</li>
                <li>• Schema validation</li>
                <li>• Duplicate detection</li>
                <li>• Missing value analysis</li>
              </ul>
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
};

export default Upload;